/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.model.{Record, StreamPosition, Timestamp}
import com.adform.streamloader.util.{MetricTag, Metrics}
import io.micrometer.core.instrument.Gauge
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Pattern
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

/**
  * A source of data for stream loading that is backed by Kafka.
  *
  * Not thread safe, create separate instances when loading data from multiple threads.
  *
  * @param consumerProperties Kafka consumer properties to use.
  * @param topics Topics to subscribe to, either a list or a pattern of topics.
  * @param pollTimeout Timeout when polling data from Kafka.
  * @param watermarkProviderFactory Factory for constructing watermark providers.
  */
class KafkaSource(
    consumerProperties: Properties,
    topics: Either[Seq[String], Pattern],
    pollTimeout: Duration,
    watermarkProviderFactory: () => WatermarkProvider)
    extends Metrics {

  override protected def metricsRoot: String = "stream_loader.source"

  private val props = new Properties()

  props.putAll(consumerProperties)
  props.put("enable.auto.commit", "false")
  props.put("key.deserializer", classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName)
  props.put("value.deserializer", classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName)

  private var consumerLock: ReentrantLock = _
  private var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _

  private val watermarkProviders: TrieMap[TopicPartition, WatermarkProvider] = TrieMap.empty
  private val watermarkMetricGauges: TrieMap[TopicPartition, Gauge] = TrieMap.empty

  private def withLock[T](code: => T): T = {
    consumerLock.lockInterruptibly()
    try {
      code
    } finally {
      consumerLock.unlock()
    }
  }

  /**
    * Initializes the source by creating a Kafka consumer using the provided configuration.
    *
    * @return A [[KafkaContext]] that can later be used for offset committing.
    */
  def initialize(): KafkaContext = {
    consumerLock = new ReentrantLock()
    consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    val consumerGroup = props.get(ConsumerConfig.GROUP_ID_CONFIG).toString
    new LockingKafkaContext(consumer, consumerGroup, consumerLock)
  }

  /**
    * Subscribes to the Kafka topics provided in the constructor.
    *
    * @param listener The callback for assignment/revocation, users should perform any needed seeking here using `seek()`.
    */
  def subscribe(listener: ConsumerRebalanceListener): Unit = {
    // do not lock as this is invoked during poll, so would result in a deadlock

    val initializingListener = new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        partitions.forEach(tp => {
          watermarkProviders.remove(tp)
          watermarkMetricGauges.remove(tp).foreach(_.close())
        })
        listener.onPartitionsRevoked(partitions)
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        partitions.forEach(tp => {
          val watermarkProvider = watermarkProviderFactory()
          watermarkProviders.addOne(tp -> watermarkProvider)
          watermarkMetricGauges.addOne(tp -> Metrics.watermarkGauge(tp, watermarkProvider))
        })
        listener.onPartitionsAssigned(partitions)
      }
    }

    topics match {
      case Left(tp) => consumer.subscribe(tp.asJava, initializingListener)
      case Right(pt) => consumer.subscribe(pt, initializingListener)
    }
  }

  /**
    * Resets the consumer offsets to the given stream position. Should be called in the subscription callback.
    *
    * @param partition Kafka topic partition to seek.
    * @param position Stream position to seek to.
    */
  def seek(partition: TopicPartition, position: StreamPosition): Unit = {
    // do not lock as this can be invoked during poll on a rebalance, so would result in a deadlock
    watermarkProviders(partition).initialize(position.watermark)
    consumer.seek(partition, position.offset)
  }

  /**
    * Polls Kafka for new records.
    *
    * @return An iterator of polled records.
    */
  def poll(): Iterable[Record] = withLock {
    // loaders can commit offsets in the background, so take a lock on the consumer
    consumer
      .poll(pollTimeout)
      .asScala
      .map(consumerRecord => {
        val partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
        val watermark = watermarkProviders(partition).observeEvent(Timestamp(consumerRecord.timestamp()))
        Record(consumerRecord, watermark)
      })
  }

  /**
    * Closes the source and all underlying resources.
    */
  def close(): Unit = withLock {
    consumer.close()
  }

  private object Metrics {

    private val commonTags = Seq(
      MetricTag("loader-thread", Thread.currentThread().getName)
    )
    private def partitionTags(tp: TopicPartition) =
      Seq(MetricTag("topic", tp.topic()), MetricTag("partition", tp.partition().toString))

    def watermarkGauge(partition: TopicPartition, watermarkProvider: WatermarkProvider): Gauge =
      createGauge(
        "watermark.ms",
        watermarkProvider,
        (p: WatermarkProvider) => p.currentWatermark.millis.toDouble,
        commonTags ++ partitionTags(partition))
  }
}

object KafkaSource {
  case class Builder(
      private val _consumerProperties: Properties,
      private val _topics: Either[Seq[String], Pattern],
      private val _pollTimeout: Duration,
      private val _watermarkProviderFactory: () => WatermarkProvider) {

    def consumerProperties(props: Properties): Builder = copy(_consumerProperties = props)
    def topics(topics: Seq[String]): Builder = copy(_topics = Left(topics))
    def topics(pattern: Pattern): Builder = copy(_topics = Right(pattern))
    def pollTimeout(timeout: Duration): Builder = copy(_pollTimeout = timeout)

    def validWatermarkDiff(diff: Duration): Builder =
      copy(_watermarkProviderFactory = () => new MaxWatermarkProvider(diff))

    def build(): KafkaSource = new KafkaSource(_consumerProperties, _topics, _pollTimeout, _watermarkProviderFactory)
  }

  def builder(): Builder =
    Builder(
      new Properties(),
      Left(Seq.empty),
      Duration.ofSeconds(1),
      () => new MaxWatermarkProvider(Duration.ofHours(1)))
}
