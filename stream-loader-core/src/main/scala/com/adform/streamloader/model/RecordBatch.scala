/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import org.apache.kafka.common.TopicPartition

import scala.collection.concurrent.TrieMap

/**
  * A base trait representing a batch of records.
  */
trait RecordBatch {

  /**
    * Gets the ranges of records in each topic partition contained in the batch.
    */
  def recordRanges: Seq[RecordRange]

  /**
    * Performs any necessary cleanup after the batch is no longer needed, e.g. deletes any underlying files.
    *
    * @return Whether the discard operation succeeded.
    */
  def discard(): Boolean
}

/**
  * A record batch builder, the base implementation takes care of counting records and keeping track
  * of contained record ranges. Concrete implementations should additionally implement actual batch construction.
  *
  * @tparam B Type of the batches being built.
  */
abstract class RecordBatchBuilder[+B <: RecordBatch] {
  private val recordRangeBuilders: TrieMap[TopicPartition, RecordRangeBuilder] = TrieMap.empty
  private var recordCount = 0L

  def currentRecordRanges: Seq[RecordRange] = recordRangeBuilders.values.map(_.build()).toSeq
  def currentRecordCount: Long = recordCount

  /**
    * Adds a new record to the current batch.
    * The default implementation only counts records added and keeps track of contained record ranges.
    */
  def add(record: Record): Unit = {
    val tp = new TopicPartition(record.consumerRecord.topic(), record.consumerRecord.partition())
    val position = StreamPosition(record.consumerRecord.offset(), record.watermark)
    val recordRangeBuilder =
      recordRangeBuilders.getOrElseUpdate(tp, new RecordRangeBuilder(tp.topic(), tp.partition(), position))
    recordRangeBuilder.extend(position)
    recordCount += 1
  }

  /**
    * Checks whether the current batch is ready. Concrete implementations can check
    * whether the batch contains a certain amount of records or some time has passed since batch creation, etc.
    */
  def isBatchReady: Boolean

  /**
    * Finalizes and outputs the batch.
    * Returns `None` if no records were added to the batch.
    */
  def build(): Option[B]

  /**
    * Discards the current batch, performs any necessary cleanup.
    */
  def discard(): Unit
}
