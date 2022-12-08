/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.model.{StreamRecord, StreamPosition}
import org.apache.kafka.common.TopicPartition

/**
  * Base sinker responsible for loading a fixed set of Kafka topic partitions to persistent storage.
  * Implementers must define committed stream position lookup from the underlying storage upon initialization.
  */
trait PartitionGroupSinker {

  /**
    * Name of the partition group, can be used in metrics and logs to differentiate between group sinkers.
    */
  val groupName: String

  /**
    * The Kafka topic partitions that this sinker is responsible for.
    */
  val groupPartitions: Set[TopicPartition]

  /**
    * Initializes the sinker and returns the stream positions where topic partitions should be reset to before loading.
    * Should be called once before any subsequent calls to [[write]].
    * This is most likely a blocking call since it queries Kafka/storage for offsets.
    *
    * @param kafkaContext Kafka context to use when looking up / committing offsets to Kafka, if needed.
    * @return The initial loader positions in the owned topic partitions.
    *         If no position is returned the position will not be reset explicitly, meaning that the consumer will
    *         either reset it to the earliest/latest based on the configuration value of `auto.offset.reset`,
    *         or will reset to the offset stored in Kafka, if any.
    */
  def initialize(kafkaContext: KafkaContext): Map[TopicPartition, Option[StreamPosition]]

  /**
    * Writes a given stream record to storage.
    *
    * Calling this method does not ensure that the record will be flushed to storage,
    * e.g. the sinker might implement batching thus delaying the actual storage.
    */
  def write(record: StreamRecord): Unit

  /**
    * Notifies the sinker that record consumption is still active.
    * Gives the sinker an opportunity to perform flushing with very low traffic streams.
    */
  def heartbeat(): Unit

  /**
    * Cleans up and closes the sinker.
    */
  def close(): Unit
}
