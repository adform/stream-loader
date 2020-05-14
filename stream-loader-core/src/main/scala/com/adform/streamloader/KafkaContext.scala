/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import java.util.concurrent.locks.ReentrantLock

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

/**
  * Represents an active Kafka consumer, can be used to commit and query offsets.
  */
trait KafkaContext {

  /**
    * The consumer group ID.
    */
  val consumerGroup: String

  /**
    * Retrieves the committed offsets for the given topic partitions from Kafka.
    */
  def committed(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[OffsetAndMetadata]]

  /**
    * Commits offsets to Kafka synchronously.
    */
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit
}

/**
  * A [[KafkaContext]] that performs offset lookup/committing by synchronizing on the consumer,
  * which out of the box is not thread safe. The source that provides this context also synchronizes
  * on the same lock during polls.
  *
  * @param consumer Kafka consumer to use.
  * @param lock Lock to synchronize on.
  */
class LockingKafkaContext(
    consumer: KafkaConsumer[Array[Byte], Array[Byte]],
    val consumerGroup: String,
    lock: ReentrantLock
) extends KafkaContext {

  def committed(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[OffsetAndMetadata]] = withLock {
    consumer.committed(topicPartitions.asJava).asScala.map(kv => (kv._1, Option(kv._2))).toMap
  }

  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = withLock {
    consumer.commitSync(offsets.asJava)
  }

  private def withLock[T](code: => T): T = {
    lock.lockInterruptibly()
    try {
      code
    } finally {
      lock.unlock()
    }
  }
}
