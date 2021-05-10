/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch

import java.time.Duration

import com.adform.streamloader.batch.storage.RecordBatchStorage
import com.adform.streamloader.model.RecordBatch
import com.adform.streamloader.util.Retry
import com.adform.streamloader.{KafkaContext, PartitionGroupSinker, PartitionGroupingSink, Sink}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._
import scala.jdk.DurationConverters._

/**
  * A [[Sink]] that uses a specified [[RecordBatcher]] to construct record batches and commit them to a specified
  * $RecordBatchStorage once they are ready.
  * It is a partition grouping sink, meaning records from each partition group are added to different batches
  * and processed separately. Batch commits to storage are queued up and performed asynchronously
  * in the background for each partition group. The commit queue size is configurable and commits block if the queues get full.
  *
  * @tparam B Type of record batches.
  * @define RecordBatchStorage [[com.adform.streamloader.batch.storage.RecordBatchStorage RecordBatchStorage]]
  */
class RecordBatchingSink[+B <: RecordBatch] protected (
    recordBatcher: RecordBatcher[B],
    batchStorage: RecordBatchStorage[B],
    batchCommitQueueSize: Int,
    partitionGrouping: TopicPartition => String,
    validWatermarkDiffMillis: Long,
    retryPolicy: Retry.Policy
) extends PartitionGroupingSink {

  override def initialize(context: KafkaContext): Unit = {
    super.initialize(context)
    batchStorage.initialize(context)
  }

  final override def groupForPartition(topicPartition: TopicPartition): String =
    partitionGrouping(topicPartition)

  final override def sinkerForPartitionGroup(
      groupName: String,
      groupPartitions: Set[TopicPartition]
  ): PartitionGroupSinker =
    new RecordBatchingSinker[B](
      groupName,
      groupPartitions,
      recordBatcher,
      batchStorage,
      batchCommitQueueSize,
      validWatermarkDiffMillis,
      retryPolicy
    )
}

object RecordBatchingSink {

  case class Builder[B <: RecordBatch](
      private val _recordBatcher: RecordBatcher[B],
      private val _batchStorage: RecordBatchStorage[B],
      private val _batchCommitQueueSize: Int,
      private val _partitionGrouping: TopicPartition => String,
      private val _validWatermarkDiffMillis: Long,
      private val _retryPolicy: Retry.Policy
  ) extends Sink.Builder {

    /**
      * Sets the record batcher to use.
      */
    def recordBatcher(batcher: RecordBatcher[B]): Builder[B] = copy(_recordBatcher = batcher)

    /**
      * Sets the storage, e.g. HDFS.
      */
    def batchStorage(storage: RecordBatchStorage[B]): Builder[B] = copy(_batchStorage = storage)

    /**
      * Sets the max number of pending batches queued to be committed to storage.
      * Consumption stops when the queue fills up.
      */
    def batchCommitQueueSize(size: Int): Builder[B] = copy(_batchCommitQueueSize = size)

    /**
      * Sets the upper limit for increasing the watermark by from the current value.
      * Used to protect from malformed messages with timestamps from the future.
      */
    def validWatermarkDiff(millis: Long): Builder[B] = copy(_validWatermarkDiffMillis = millis)

    /**
      * Sets the upper limit for increasing the watermark by from the current value.
      * Used to protect from malformed messages with timestamps from the future.
      */
    def validWatermarkDiff(duration: Duration): Builder[B] = copy(_validWatermarkDiffMillis = duration.toMillis)

    /**
      * Sets the retry policy for all retriable operations, i.e. recovery, batch commit and new batch creation.
      */
    def retryPolicy(retries: Int, initialDelay: Duration, backoffFactor: Int): Builder[B] =
      copy(_retryPolicy = Retry.Policy(retries, initialDelay.toScala, backoffFactor))

    /**
      * Sets the partition grouping, can be used to route records to different batches.
      */
    def partitionGrouping(grouping: TopicPartition => String): Builder[B] = copy(_partitionGrouping = grouping)

    def build(): RecordBatchingSink[B] = {
      if (_recordBatcher == null) throw new IllegalStateException("Must specify a RecordBatcher")
      if (_batchStorage == null) throw new IllegalStateException("Must specify a RecordBatchStorage")

      new RecordBatchingSink[B](
        _recordBatcher,
        _batchStorage,
        _batchCommitQueueSize,
        _partitionGrouping,
        _validWatermarkDiffMillis,
        _retryPolicy
      )
    }
  }

  def builder[B <: RecordBatch](): Builder[B] = Builder[B](
    _recordBatcher = null,
    _batchStorage = null,
    _batchCommitQueueSize = 1,
    _partitionGrouping = _ => "root",
    _validWatermarkDiffMillis = 3600000,
    _retryPolicy = Retry.Policy(retriesLeft = 5, initialDelay = 1.seconds, backoffFactor = 3)
  )
}
