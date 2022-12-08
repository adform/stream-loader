/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import java.util.Properties
import com.adform.streamloader.fixtures.{Container, ContainerWithEndpoint, DockerNetwork}
import com.adform.streamloader.model.{StorageMessage, StreamPosition}
import com.adform.streamloader.source.KafkaContext
import com.spotify.docker.client.DockerClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}

import scala.jdk.CollectionConverters._

case class LoaderKafkaConfig(consumerGroup: String, topic: String)

case class StorageContent[M <: StorageMessage](
    messages: Seq[M],
    committedPositions: Map[TopicPartition, StreamPosition]
)

trait StorageBackend[M <: StorageMessage] {

  def initialize(): Unit

  def docker: DockerClient

  def dockerNetwork: DockerNetwork

  def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container

  def committedPositions(
      loaderKafkaConfig: LoaderKafkaConfig,
      partitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]]

  def arbMessage: Arbitrary[M]

  def generateRandomMessages(n: Int, seed: Long): Seq[M] = {
    LazyList
      .from(1)
      .map(i => arbMessage.arbitrary.apply(Gen.Parameters.default, Seed(i)))
      .collect { case Some(x) => x }
      .take(n)
  }

  def getContent: StorageContent[M]

  protected def getKafkaContext(kafka: ContainerWithEndpoint, consumerGroupId: String): KafkaContext = {
    val props = new Properties() {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.endpoint)
      put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
      put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName
      )
      put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName
      )
    }
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    new KafkaContext {
      override val consumerGroup: String = consumerGroupId
      override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {}
      override def committed(tps: Set[TopicPartition]): Map[TopicPartition, Option[OffsetAndMetadata]] = {
        consumer.committed(tps.asJava).asScala.map(kv => (kv._1, Option(kv._2))).toMap
      }
    }
  }
}
