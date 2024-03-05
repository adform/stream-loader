/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import com.adform.streamloader.fixtures._
import com.adform.streamloader.model.{StreamPosition, StringMessage, Timestamp}
import com.adform.streamloader.s3.S3FileStorage
import com.adform.streamloader.sink.file.{FilePathFormatter, TimePartitioningFilePathFormatter}
import com.adform.streamloader.{BuildInfo, Loader}
import com.spotify.docker.client.DockerClient
import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Arbitrary
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.utils.IoUtils

import java.time.LocalDate
import java.util.UUID
import scala.jdk.CollectionConverters._

case class LoaderS3Config(accessKey: String, secretKey: String, bucket: String)

case class S3StorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    s3Container: ContainerWithEndpoint,
    s3Config: LoaderS3Config,
    s3Client: S3Client,
    loader: Loader
) extends StorageBackend[StringMessage] {

  override def arbMessage: Arbitrary[StringMessage] = StringMessage.arbMessage

  private val timePartitionPathPattern = "'dt='yyyy'_'MM'_'dd"
  private val pathFormatter: FilePathFormatter[LocalDate] =
    new TimePartitioningFilePathFormatter(Some(timePartitionPathPattern), None)

  override def initialize(): Unit = {
    s3Client
      .createBucket(
        CreateBucketRequest
          .builder()
          .bucket(s3Config.bucket)
          .build()
      )
  }

  def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container = {
    val consumerGroup = loaderKafkaConfig.consumerGroup
    val topic = loaderKafkaConfig.topic
    val loaderName = s"s3-partition-loader-${UUID.randomUUID().toString.take(6)}"

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
        "APP_OPTS=-Dconfig.resource=application-s3.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=$topic",
        s"KAFKA_CONSUMER_GROUP=$consumerGroup",
        s"S3_ENDPOINT=http://${s3Container.ip}:${s3Container.port}",
        s"S3_ACCESS_KEY=${s3Config.accessKey}",
        s"S3_SECRET_KEY=${s3Config.secretKey}",
        s"S3_BUCKET=${s3Config.bucket}",
        s"BATCH_SIZE=$batchSize",
        s"TIME_PARTITION_PATTERN=$timePartitionPathPattern"
      )
      .build()

    val containerCreation = docker.createContainer(config, loaderName)
    SimpleContainer(containerCreation.id, loaderName)
  }

  def committedPositions(
      loaderKafkaConfig: LoaderKafkaConfig,
      partitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]] = {

    val kafkaContext = getKafkaContext(kafkaContainer, loaderKafkaConfig.consumerGroup)
    val storage = S3FileStorage
      .builder()
      .s3Client(s3Client)
      .bucket(s3Config.bucket)
      .filePathFormatter(pathFormatter)
      .build()

    storage.initialize(kafkaContext)
    storage.committedPositions(partitions)
  }

  override def getContent: StorageContent[StringMessage] = {
    val objects = listObjects(s3Config.bucket)
    val lines = objects.flatMap(o => IoUtils.toUtf8String(getObject(s3Config.bucket, o.key())).split("\n"))

    val parsedMessages = lines.map(m => {
      val parts = m.split(";")
      val tp = new TopicPartition(parts(0), parts(1).toInt)
      val position = StreamPosition(parts(2).toLong, Timestamp(parts(3).toLong))
      val msg = parts.drop(4).mkString(";")
      (msg, tp, position)
    })

    val positions = parsedMessages
      .map(p => (p._2, p._3))
      .groupBy(p => p._1)
      .map(o => (o._1, o._2.maxBy(_._2.offset)._2))

    StorageContent(parsedMessages.map(p => StringMessage(p._1)), positions)
  }

  private def listObjects(bucket: String): List[S3Object] = {
    val iterator = s3Client
      .listObjectsV2Paginator(
        ListObjectsV2Request
          .builder()
          .maxKeys(Int.MaxValue)
          .bucket(bucket)
          .build()
      )

    val objects = List.newBuilder[S3Object]
    iterator.stream.forEach(p => objects ++= p.contents.asScala)

    objects.result()
  }

  private def getObject(bucket: String, key: String): ResponseInputStream[GetObjectResponse] =
    s3Client
      .getObject(
        GetObjectRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .build()
      )
}
