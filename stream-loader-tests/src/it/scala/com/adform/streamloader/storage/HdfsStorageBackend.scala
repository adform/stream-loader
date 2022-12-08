/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import java.util.UUID
import com.adform.streamloader.fixtures._
import com.adform.streamloader.hadoop.HadoopFileStorage
import com.adform.streamloader.model.{ExampleMessage, StreamPosition}
import com.adform.streamloader.sink.file.{FilePathFormatter, TimePartitioningFilePathFormatter}
import com.adform.streamloader.{BuildInfo, Loader}
import com.sksamuel.avro4s.{RecordFormat, ScalePrecision}
import com.spotify.docker.client.DockerClient
import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.kafka.common.TopicPartition
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalacheck.Arbitrary

import java.time.LocalDate
import scala.math.BigDecimal.RoundingMode.RoundingMode

case class HdfsStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    namenodeContainer: ContainerWithEndpoint,
    datanodeContainer: Container,
    hdfs: FileSystem,
    baseDir: String,
    loader: Loader
) extends StorageBackend[ExampleMessage] {

  override def arbMessage: Arbitrary[ExampleMessage] = ExampleMessage.arbMessage

  private val timePartitionPathPattern = "'dt='yyyy'_'MM'_'dd"
  private val pathFormatter: TimePartitioningFilePathFormatter[LocalDate] =
    new TimePartitioningFilePathFormatter(Some(timePartitionPathPattern), None)

  implicit private val scalePrecision: ScalePrecision = ExampleMessage.SCALE_PRECISION
  implicit private val roundingMode: RoundingMode = ExampleMessage.ROUNDING_MODE

  override def initialize(): Unit = {
    hdfs.mkdirs(new Path(baseDir))
  }

  override def getContent: StorageContent[ExampleMessage] = {
    val format = RecordFormat[ExampleMessage]
    val messages = listFiles().flatMap { f =>
      val reader = AvroParquetReader
        .builder[GenericRecord](HadoopInputFile.fromPath(f.getPath, hdfs.getConf))
        .withDataModel(GenericData.get())
        .build()
      Iterator.continually(reader.read).takeWhile(_ != null).map(format.from)
    }
    StorageContent(messages, Map.empty)
  }

  override def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container = {
    val containerName = s"hdfs_loader_${UUID.randomUUID().toString}"
    val containerDef = ContainerConfig
      .builder()
      .image(BuildInfo.dockerImage)
      .env(
        s"APP_MAIN_CLASS=${loader.getClass.getName.replace("$", "")}",
        "APP_OPTS=-Dconfig.resource=application-hdfs.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=${loaderKafkaConfig.topic}",
        s"KAFKA_CONSUMER_GROUP=${loaderKafkaConfig.consumerGroup}",
        s"HDFS_URI=hdfs://${namenodeContainer.ip}:${namenodeContainer.port}",
        s"BATCH_SIZE=$batchSize",
        s"TIME_PARTITION_PATTERN=$timePartitionPathPattern",
        s"BASE_DIRECTORY=$baseDir"
      )
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .links(namenodeContainer.name)
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
    val storage = HadoopFileStorage
      .builder()
      .hadoopFS(hdfs)
      .stagingBasePath("/tmp")
      .destinationBasePath(baseDir)
      .destinationFilePathFormatter(pathFormatter)
      .build()

    storage.initialize(kafkaContext)
    storage.committedPositions(partitions)
  }

  protected def listFiles(): List[LocatedFileStatus] = {
    val basePath = new Path(baseDir)
    if (hdfs.exists(new Path(baseDir))) {
      toIterator(hdfs.listFiles(basePath, true)).toList
    } else {
      List.empty
    }
  }

  private def toIterator[T](it: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = it.hasNext
    override def next(): T = it.next()
  }
}
