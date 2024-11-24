/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import com.adform.streamloader.fixtures.{Container, ContainerWithEndpoint, DockerNetwork, SimpleContainer}
import com.adform.streamloader.iceberg.IcebergRecordBatchStorage
import com.adform.streamloader.model.{ExampleMessage, StreamPosition}
import com.adform.streamloader.{BuildInfo, Loader}
import com.sksamuel.avro4s.{ScalePrecision, SchemaFor}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.kafka.common.TopicPartition
import org.duckdb.{DuckDBArray, DuckDBConnection}
import org.mandas.docker.client.DockerClient
import org.mandas.docker.client.messages.{ContainerConfig, HostConfig}
import org.scalacheck.Arbitrary

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.DriverManager
import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.util.UUID
import java.util.zip.GZIPInputStream
import scala.math.BigDecimal.RoundingMode.RoundingMode
import scala.util.Using

case class IcebergStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    loader: Loader,
    table: String,
    commitDelay: Duration
) extends StorageBackend[ExampleMessage] {

  implicit private val scalePrecision: ScalePrecision = ExampleMessage.SCALE_PRECISION
  implicit private val roundingMode: RoundingMode = ExampleMessage.ROUNDING_MODE

  private val duckdbExtension = new File("/tmp/iceberg.duckdb_extension")
  private val duckdbExtensionUrl = new URI(
    s"http://extensions.duckdb.org/v${BuildInfo.duckdbVersion}/linux_amd64_gcc4/iceberg.duckdb_extension.gz"
  ).toURL

  private val warehouseDir = "/tmp/stream-loader-tests"

  private lazy val catalog = {
    Files.createDirectories(Paths.get(warehouseDir))
    new HadoopCatalog(new Configuration(), warehouseDir)
  }

  override def arbMessage: Arbitrary[ExampleMessage] = ExampleMessage.arbMessage

  override def initialize(): Unit = {
    val name = TableIdentifier.parse(table)
    val schema = AvroSchemaUtil.toIceberg(SchemaFor[ExampleMessage].schema)
    val partitionSpec = PartitionSpec.builderFor(schema).bucket("id", 10).build()

    catalog.createTable(name, schema, partitionSpec)

    // Installing the Iceberg extension from upstream via JDBC seems to fail randomly,
    // hence we download the extension and install it from a local path.
    synchronized {
      if (!duckdbExtension.exists()) {
        Using.Manager { use =>
          val stream = use(duckdbExtensionUrl.openStream())
          val unzipped = use(new GZIPInputStream(stream))
          Files.copy(unzipped, duckdbExtension.toPath, StandardCopyOption.REPLACE_EXISTING)
        }
      }
    }
  }

  override def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container = {
    val containerName = s"iceberg_loader_${UUID.randomUUID().toString}"
    val containerDef = ContainerConfig
      .builder()
      .image(BuildInfo.dockerImage)
      .env(
        s"APP_MAIN_CLASS=${loader.getClass.getName.replace("$", "")}",
        "APP_OPTS=-Dconfig.resource=application-iceberg.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=${loaderKafkaConfig.topic}",
        s"KAFKA_CONSUMER_GROUP=${loaderKafkaConfig.consumerGroup}",
        s"BATCH_SIZE=$batchSize",
        s"ICEBERG_WAREHOUSE_DIR=$warehouseDir",
        s"ICEBERG_TABLE=$table",
        s"ICEBERG_COMMIT_DELAY_MS=${commitDelay.toMillis}"
      )
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .binds(s"$warehouseDir:$warehouseDir")
          .build()
      )
      .build()

    val containerCreation = docker.createContainer(containerDef, containerName)
    SimpleContainer(containerCreation.id, containerName)
  }

  override def committedPositions(
      loaderKafkaConfig: LoaderKafkaConfig,
      partitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]] = {
    val kafkaContext = getKafkaContext(kafkaContainer, loaderKafkaConfig.consumerGroup)
    val storage = new IcebergRecordBatchStorage(catalog.loadTable(TableIdentifier.parse(table)), None)

    storage.initialize(kafkaContext)
    storage.committedPositions(partitions)
  }

  override def getContent: StorageContent[ExampleMessage] = Using.Manager { use =>
    val conn = use(DriverManager.getConnection("jdbc:duckdb:").asInstanceOf[DuckDBConnection])

    // Querying complex types from Iceberg tables is semi-broken,
    // see: https://github.com/duckdb/duckdb_iceberg/issues/47
    val stmt = use(conn.createStatement())
    val rs = use(
      stmt.executeQuery(
        s"""INSTALL '${duckdbExtension.getPath}';
           |LOAD iceberg;
           |SELECT * FROM iceberg_scan('$warehouseDir/${table.replace(
            '.',
            '/'
          )}', skip_schema_inference=True);""".stripMargin
      )
    )

    val buff = scala.collection.mutable.ListBuffer.empty[ExampleMessage]

    while (rs.next()) {
      val msg = ExampleMessage(
        rs.getInt(1),
        rs.getString(2),
        LocalDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong(3)), ZoneId.of("UTC")),
        rs.getDouble(4),
        rs.getFloat(5),
        rs.getBoolean(6),
        rs.getArray(7).asInstanceOf[DuckDBArray].getArray.asInstanceOf[Array[Object]].map(_.asInstanceOf[Int]),
        Option(rs.getLong(8)),
        rs.getObject(9).asInstanceOf[UUID],
        rs.getBigDecimal(10)
      )
      buff.addOne(msg)
    }

    StorageContent(buff.toSeq, Map.empty)
  }.get
}
