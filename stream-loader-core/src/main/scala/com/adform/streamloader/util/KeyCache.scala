/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import io.micrometer.core.instrument.Gauge

import scala.collection.mutable

/**
  * Interface hiding cache implementations
  *
  * @tparam A
  */
trait KeyCache[A] {
  def assignPartition(partition: Int, startOffset: Long, ready: Boolean): Unit
  def revokePartitions(partition: Int): Unit
  def clearCache(): Unit
  def add(partition: Int, key: A): Unit
  def contains(partition: Int, key: A): Boolean
  def verifyAndSwitchIfReady(partition: Int, offset: Long): Boolean
  def keysSize(): Int
  def partitionSize(partition: Int): Int
}

object KeyCache {
  def perPartition[A](cacheSize: Int): KeyCache[A] = PerPartitionKeyCache[A](cacheSize)
  def noop[A](): KeyCache[A] = NoopKeyCache[A]()
}

/**
  * Not thread safe
  *
  * Create separate FIFO HashSet per partition
  * KafkaSource polls data in uneven manner for each partition, to be sure that we build cache properly
  * we verify partition for each record and add it to proper Set
  *
  * @param cacheSize - number of cached keys per partition
  * @tparam A - key type
  */
class PerPartitionKeyCache[A] private (cacheSize: Int, val cacheForPartition: mutable.Map[Int, PartitionData[A]])
    extends KeyCache[A]
    with Logging {

  /**
    * Called on `ConsumerRebalanceListener.onPartitionsAssigned`
    *
    * Two scenarios for initialization:
    *  - there is lastOffset - fill cache to `cacheSize` before marking cache for partition as `ready`
    *  - no information about lastOffset - mark cache for partition as `ready` and fill cache as we Sink data
    *
    * @see {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(Collection<TopicPartition> partitions) }
    * @param partition - partition assigned to consumer
    * @param ready - is ready for consumption
    */
  override def assignPartition(partition: Int, startOffset: Long, ready: Boolean): Unit = {
    cacheForPartition.addOne((partition, PartitionData[A](ready, startOffset, FifoHashSet[A](cacheSize))))
  }

  /**
    * Called on `ConsumerRebalanceListener.onPartitionsRevoked`
    *
    * @see {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(Collection<TopicPartition> partitions) }
    * @param partition - partition revoked from consumer
    */
  override def revokePartitions(partition: Int): Unit = {
    cacheForPartition.remove(partition)
  }

  override def clearCache(): Unit = cacheForPartition.clear()

  override def add(partition: Int, key: A): Unit = cacheForPartition(partition).addKey(key)

  override def contains(partition: Int, key: A): Boolean = cacheForPartition(partition).containsKey(key)

  override def verifyAndSwitchIfReady(partition: Int, offset: Long): Boolean = {
    val partitionData = cacheForPartition(partition)
    if (partitionData.isReady) {
      true
    } else if (partitionData.startOffset < offset) {
      partitionData.isReady = true
      log.info(s"Cache for partition $partition ready: ${partitionData.keySize()} ")
      partitionData.isReady
    } else {
      false
    }
  }

  override def keysSize(): Int = cacheSize

  override def partitionSize(partition: Int): Int = cacheForPartition(partition).keySize()
}

object PerPartitionKeyCache {
  def apply[A](cacheSize: Int): PerPartitionKeyCache[A] =
    new PerPartitionKeyCache[A](cacheSize, mutable.Map[Int, PartitionData[A]]())
}

class NoopKeyCache[A] private () extends KeyCache[A] {
  override def assignPartition(partition: Int, startOffset: Long, ready: Boolean): Unit = {}

  override def revokePartitions(partition: Int): Unit = {}

  override def clearCache(): Unit = {}

  override def add(partition: Int, key: A): Unit = {}

  override def contains(partition: Int, key: A): Boolean = false

  override def verifyAndSwitchIfReady(partition: Int, offset: Long): Boolean = true

  override def keysSize(): Int = 0

  override def partitionSize(partition: Int): Int = 0
}

object NoopKeyCache {
  def apply[A](): NoopKeyCache[A] = new NoopKeyCache()
}

case class PartitionData[A](var isReady: Boolean, startOffset: Long, keys: FifoHashSet[A]) {
  def addKey(a: A): Unit = {
    keys.add(a)
  }
  def containsKey(a: A): Boolean = keys.contains(a)
  def keySize(): Int = keys.size()
}
