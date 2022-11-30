/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import org.apache.kafka.common.TopicPartition

/**
  * Interface hiding cache implementations
  *
  * @tparam A
  */
trait KeyCache[A] {
  def initCache(partitions: Set[TopicPartition]): Unit
  def clearCache(): Unit
  def add(partition: Int, key: A): Unit
  def contains(partition: Int, key: A): Boolean
  def ready(partition: Int): Boolean
  def size(): Int
}

object KeyCache {
  def perPartition[A](cacheSize: Int): KeyCache[A] = PerPartitionKeyCache[A](cacheSize)
  def noop[A](): KeyCache[A] = NoopKeyCache[A]()
}

/**
  * Not thread safe
  *
  * Create separate FIFO HashSet per partition
  *
  * @param cacheSize - number of cached keys per partition
  * @tparam A
  */
class PerPartitionKeyCache[A] private (cacheSize: Int) extends KeyCache[A] {

  var cacheForPartition: Map[Int, FifoHashSet[A]] = _

  //TODO if no offset set as partition as ready
  override def initCache(partitions: Set[TopicPartition]): Unit =
    cacheForPartition = partitions.map(p => (p.partition(), FifoHashSet[A](cacheSize))).toMap

  override def clearCache(): Unit = cacheForPartition.values.foreach(_.clear())

  override def add(partition: Int, key: A): Unit = cacheForPartition(partition).add(key)

  override def contains(partition: Int, key: A): Boolean = cacheForPartition(partition).contains(key)

  override def ready(partition: Int): Boolean = cacheSize == cacheForPartition(partition).size()

  override def size(): Int = cacheSize
}

object PerPartitionKeyCache {
  def apply[A](cacheSize: Int): PerPartitionKeyCache[A] = new PerPartitionKeyCache[A](cacheSize)
}

class NoopKeyCache[A] private () extends KeyCache[A] {
  override def initCache(partitions: Set[TopicPartition]): Unit = {}

  override def clearCache(): Unit = {}

  override def add(partition: Int, key: A): Unit = {}

  override def contains(partition: Int, key: A): Boolean = false

  override def ready(partition: Int): Boolean = true

  override def size(): Int = 0
}

object NoopKeyCache {
  def apply[A](): NoopKeyCache[A] = new NoopKeyCache()
}
