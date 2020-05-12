/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import com.adform.streamloader.KafkaContext
import com.adform.streamloader.file.RecordRangeFile
import com.adform.streamloader.model.StreamPosition
import org.apache.kafka.common.TopicPartition

/**
  * A persistent file storage that knows how to persist files and look up the last committed stream positions.
  * Optionally the storage might provide means to generate unique IDs within the storage.
  * Users should first `initialize` and `recover` before using the storage.
  */
trait FileStorage[F] {

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
    * Starts a new file within the storage and returns an associated ID.
    */
  def startNewFile(): F

  /**
    * Commits a given file to storage.
    */
  def commitFile(file: RecordRangeFile[F]): Unit

  /**
    * Gets the latest committed stream positions for the given partitions where streams should be seeked to,
    * i.e. this should be the last stored offset + 1.
    */
  def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]]

  /**
    * Checks whether a given file was successfully committed to storage by comparing committed positions
    * with the record ranges in the file.
    *
    * @param file File to check.
    * @return Whether the file is committed.
    */
  def isFileCommitted(file: RecordRangeFile[F]): Boolean = {
    val partitions = file.recordRanges.map(r => new TopicPartition(r.topic, r.partition)).toSet
    val positions = committedPositions(partitions)
    file.recordRanges.forall(r => {
      val committed = positions(new TopicPartition(r.topic, r.partition))
      committed.exists(cp => cp > r.end) // we commit r.end + 1, so check for a greater offset
    })
  }
}
