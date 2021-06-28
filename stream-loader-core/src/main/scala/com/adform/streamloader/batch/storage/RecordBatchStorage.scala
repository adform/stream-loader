/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch.storage

import com.adform.streamloader.KafkaContext
import com.adform.streamloader.model._
import org.apache.kafka.common.TopicPartition

/**
  * A record batch storage that stores batches to some underlying persistent storage and commits offsets,
  * preferably atomically, if exactly-once guarantees are required.
  *
  * @tparam B Type of record batches that the storage accepts.
  */
trait RecordBatchStorage[-B <: RecordBatch] {

  protected var kafkaContext: KafkaContext = _

  /**
    * Initializes the storage with a Kafka context, which can be used to lookup/commit offsets, if needed.
    */
  def initialize(context: KafkaContext): Unit = {
    kafkaContext = context
  }

  /**
    * Performs any needed recovery upon startup, e.g. rolling back or completing transactions.
    * Can fail, users should handle any possible exceptions.
    */
  def recover(topicPartitions: Set[TopicPartition]): Unit

  /**
    * Stores a given batch to storage and commits offsets, preferably in a single atomic transaction.
    */
  def commitBatch(batch: B): Unit

  /**
    * Gets the latest committed stream positions for the given partitions where streams should be sought to,
    * i.e. this should be the last stored offset + 1.
    */
  def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]]

  /**
    * Checks whether a given batch was successfully committed to storage by comparing committed positions
    * with the record ranges in the batch.
    *
    * @param batch Batch to check.
    * @return Whether the batch is successfully stored.
    */
  def isBatchCommitted(batch: B): Boolean = {
    val partitions = batch.recordRanges.map(r => new TopicPartition(r.topic, r.partition)).toSet
    val positions = committedPositions(partitions)
    batch.recordRanges.forall(r => {
      val committed = positions(new TopicPartition(r.topic, r.partition))
      committed.exists(cp => cp > r.end) // we commit r.end + 1, so check for a greater offset
    })
  }
}
