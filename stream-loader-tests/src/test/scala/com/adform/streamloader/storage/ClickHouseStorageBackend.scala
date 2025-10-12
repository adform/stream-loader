/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import com.adform.streamloader.clickhouse.ClickHouseFileStorage
import com.adform.streamloader.fixtures._
import com.adform.streamloader.model.{ExampleMessage, StreamPosition, Timestamp}
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.Retry
import com.adform.streamloader.{BuildInfo, Loader}
import com.clickhouse.client.api.Client
import com.clickhouse.client.api.query.QuerySettings
import org.apache.kafka.common.TopicPartition
import org.mandas.docker.client.DockerClient
import org.mandas.docker.client.messages.{ContainerConfig, HostConfig}
import org.scalacheck.Arbitrary

import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

case class ClickHouseStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    clickHouseContainer: ContainerWithEndpoint,
    clickHouseConfig: ClickHouseConfig,
    clickHouseClient: Client,
    table: String,
    loader: Loader
) extends StorageBackend[ExampleMessage] {

  override def arbMessage: Arbitrary[ExampleMessage] = ExampleMessage.arbMessage

  // ClickHouse stores timestamps without milliseconds, so we truncate them
  override def generateRandomMessages(n: Int, seed: Long): Seq[ExampleMessage] = {
    val generated = super.generateRandomMessages(n, seed)
    generated.map(m => m.copy(timestamp = m.timestamp.truncatedTo(ChronoUnit.SECONDS)))
  }

  val TOPIC_COLUMN = "_topic"
  val PARTITION_COLUMN = "_partition"
  val OFFSET_COLUMN = "_offset"
  val WATERMARK_COLUMN = "_watermark"

  val kafkaContext: KafkaContext = getKafkaContext(kafkaContainer, "test")

  val batchStorage: ClickHouseFileStorage = ClickHouseFileStorage
    .builder()
    .client(clickHouseClient)
    .table(table)
    .rowOffsetColumnNames(TOPIC_COLUMN, PARTITION_COLUMN, OFFSET_COLUMN, WATERMARK_COLUMN)
    .build()

  override def initialize(): Unit = {
    batchStorage.initialize(kafkaContext)
    clickHouseClient
      .execute(
        s"""CREATE TABLE IF NOT EXISTS $table (
           |  $TOPIC_COLUMN String,
           |  $PARTITION_COLUMN UInt16,
           |  $OFFSET_COLUMN UInt64,
           |  $WATERMARK_COLUMN Timestamp,
           |  id Int32,
           |  name String,
           |  timestamp Timestamp,
           |  height Float64,
           |  width Float32,
           |  is_enabled UInt8,
           |  child_ids Array(Int32),
           |  parent_id Nullable(Int64),
           |  transaction_id UUID,
           |  money_spent Decimal(${ExampleMessage.SCALE_PRECISION.precision}, ${ExampleMessage.SCALE_PRECISION.scale})
           |) ENGINE = MergeTree()
           |ORDER BY $OFFSET_COLUMN;""".stripMargin
      )
      .get()
  }

  def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container = {
    val consumerGroup = loaderKafkaConfig.consumerGroup
    val topic = loaderKafkaConfig.topic
    val loaderName = s"clickhouse-loader-${UUID.randomUUID().toString.take(6)}"

    val config = ContainerConfig
      .builder()
      .image(BuildInfo.dockerImage)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .build()
      )
      .env(
        s"APP_MAIN_CLASS=${loader.getClass.getName.replace("$", "")}",
        "APP_OPTS=-Dconfig.resource=application-clickhouse.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=$topic",
        s"KAFKA_CONSUMER_GROUP=$consumerGroup",
        s"CLICKHOUSE_HOST=${clickHouseContainer.ip}",
        s"CLICKHOUSE_PORT=${clickHouseContainer.port}",
        s"CLICKHOUSE_DB=${clickHouseConfig.dbName}",
        s"CLICKHOUSE_USER=${clickHouseConfig.userName}",
        s"CLICKHOUSE_PASSWORD=${clickHouseConfig.password}",
        s"CLICKHOUSE_TABLE=$table",
        s"BATCH_SIZE=$batchSize"
      )
      .build()

    val containerCreation = docker.createContainer(config, loaderName)
    SimpleContainer(containerCreation.id, loaderName)
  }

  override def getContent: StorageContent[ExampleMessage] =
    Retry.retryOnFailure(Retry.Policy(retriesLeft = 3, initialDelay = 1.seconds, backoffFactor = 1)) {
      val content: ListBuffer[ExampleMessage] = collection.mutable.ListBuffer[ExampleMessage]()
      val positions: mutable.HashMap[TopicPartition, ListBuffer[StreamPosition]] = mutable.HashMap.empty

      clickHouseClient
        .queryAll(s"SELECT * FROM $table", new QuerySettings().setMaxExecutionTime(5))
        .forEach(row => {
          val topicPartition = new TopicPartition(row.getString(TOPIC_COLUMN), row.getInteger(PARTITION_COLUMN))
          val position =
            StreamPosition(
              row.getLong(OFFSET_COLUMN),
              Timestamp(row.getLocalDateTime(WATERMARK_COLUMN).toInstant(ZoneOffset.UTC).toEpochMilli)
            )

          positions
            .getOrElseUpdate(topicPartition, collection.mutable.ListBuffer[StreamPosition]())
            .append(position)

          content.addOne(
            ExampleMessage(
              row.getInteger("id"),
              row.getString("name"),
              row.getLocalDateTime("timestamp"),
              row.getDouble("height"),
              row.getFloat("width"),
              row.getBoolean("is_enabled"),
              row.getIntArray("child_ids"),
              Option(row.getObject("parent_id").asInstanceOf[java.lang.Long]).map(_.toLong),
              row.getObject("transaction_id").asInstanceOf[UUID],
              row.getBigDecimal("money_spent")
            )
          )
        })

      StorageContent(content.toList, positions.view.mapValues(sps => sps.maxBy(_.offset)).toMap)
    }

  override def committedPositions(
      loaderKafkaConfig: LoaderKafkaConfig,
      partitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]] =
    batchStorage.committedPositions(partitions)
}
