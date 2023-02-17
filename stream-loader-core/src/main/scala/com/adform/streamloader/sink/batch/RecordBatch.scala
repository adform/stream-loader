/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch

import com.adform.streamloader.model.{StreamPosition, StreamRange, StreamRangeBuilder, StreamRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
  * A base trait representing a batch of records.
  */
trait RecordBatch {

  /**
    * Gets the ranges of records in each topic partition contained in the batch.
    */
  def recordRanges: Seq[StreamRange]

  /**
    * Performs any necessary cleanup after the batch is no longer needed, e.g. deletes any underlying files.
    *
    * @return Whether the discard operation succeeded.
    */
  def discard(): Boolean
}

/**
  * A record batch builder, the base implementation takes care of keeping track of contained record ranges.
  * Concrete implementations should additionally implement actual batch construction.
  *
  * @tparam B Type of the batches being built.
  */
abstract class RecordBatchBuilder[+B <: RecordBatch] {
  private val recordRangeBuilders: mutable.HashMap[TopicPartition, StreamRangeBuilder] = mutable.HashMap.empty

  def currentRecordRanges: Seq[StreamRange] = recordRangeBuilders.values.map(_.build()).toSeq

  /**
    * Adds a new record to the current batch.
    * Keeps track of record ranges in the batch and delegates the actual adding to [[addToBatch]].
    *
    * @param record Record to add to batch.
    * @return Actual number of records added to the batch.
    */
  def add(record: StreamRecord): Int = {
    val tp = new TopicPartition(record.consumerRecord.topic(), record.consumerRecord.partition())
    val position = StreamPosition(record.consumerRecord.offset(), record.watermark)
    val recordRangeBuilder =
      recordRangeBuilders.getOrElseUpdate(tp, new StreamRangeBuilder(tp.topic(), tp.partition(), position))
    recordRangeBuilder.extend(position)
    addToBatch(record)
  }

  /**
    * Adds a record to the underlying batch and returns the number of records actually added.
    */
  protected def addToBatch(record: StreamRecord): Int

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
