/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.time.Duration

import com.adform.streamloader.file.storage.FileStorage
import com.adform.streamloader.{KafkaContext, PartitionGroupSinker, PartitionGroupingSink, Sink}
import org.apache.kafka.common.TopicPartition

/**
  * A [[Sink]] that writes records to files and stores them to some underlying storage.
  * It is a partition grouping sink where records from each group are written to separate files.
  * Once a user defined threshold is reached files are closed and queued for sequential committing to storage in a background thread.
  * Records retrieved from the sink are formatted, e.g. by filtering, exploding, etc., encoded and finally written to the file.
  *
  * @tparam R Type of records written to the file.
  * @tparam F Type of file IDs provided by the storage.
  */
class FileSink[-R, F] private (
    fileStorage: FileStorage[F],
    fileBuilderFactory: FileBuilderFactory[R],
    fileRecordFormatter: FileRecordFormatter[R, F],
    partitionGrouping: TopicPartition => String,
    fileCommitStrategy: FileCommitStrategy,
    fileCommitQueueSize: Int,
    validWatermarkDiffMillis: Long
) extends PartitionGroupingSink {

  override def initialize(context: KafkaContext): Unit = {
    super.initialize(context)
    fileStorage.initialize(context)
  }

  final override def groupForPartition(topicPartition: TopicPartition): String =
    partitionGrouping(topicPartition)

  final override def sinkerForPartitionGroup(
      groupName: String,
      groupPartitions: Set[TopicPartition]): PartitionGroupSinker =
    new FilePartitionGroupSinker(
      groupName,
      groupPartitions,
      fileRecordFormatter,
      fileBuilderFactory,
      fileStorage,
      fileCommitStrategy,
      fileCommitQueueSize,
      validWatermarkDiffMillis
    )
}

object FileSink {

  case class Builder[R, F](
      private val _fileStorage: FileStorage[F],
      private val _fileBuilderFactory: FileBuilderFactory[R],
      private val _recordFormatter: FileRecordFormatter[R, F],
      private val _partitionGrouping: TopicPartition => String,
      private val _fileCommitStrategy: FileCommitStrategy,
      private val _fileCommitQueueSize: Int,
      private val _validWatermarkDiffMillis: Long
  ) extends Sink.Builder {

    /**
      * Sets the file storage, e.g. HDFS.
      */
    def fileStorage(storage: FileStorage[F]): Builder[R, F] = copy(_fileStorage = storage)

    /**
      * Sets the file builder factory, e.g. CSV.
      */
    def fileBuilderFactory(factory: FileBuilderFactory[R]): Builder[R, F] = copy(_fileBuilderFactory = factory)

    /**
      * Sets the strategy for determining if a file is ready to be committed to storage.
      */
    def fileCommitStrategy(strategy: FileCommitStrategy): Builder[R, F] = copy(_fileCommitStrategy = strategy)

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: FileRecordFormatter[R, F]): Builder[R, F] = copy(_recordFormatter = formatter)

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: RecordFormatter[R]): Builder[R, F] =
      copy(_recordFormatter = formatter.toFileRecordFormatter[F])

    /**
      * Sets the max number of pending files queued to be committed to storage.
      * Consumption stops when the queue fills up.
      */
    def fileCommitQueueSize(size: Int): Builder[R, F] = copy(_fileCommitQueueSize = size)

    /**
      * Sets the upper limit for increasing the watermark by from the current value.
      * Used to protect from malformed messages with timestamps from the future.
      */
    def validWatermarkDiff(millis: Long): Builder[R, F] = copy(_validWatermarkDiffMillis = millis)

    /**
      * Sets the upper limit for increasing the watermark by from the current value.
      * Used to protect from malformed messages with timestamps from the future.
      */
    def validWatermarkDiff(duration: Duration): Builder[R, F] = copy(_validWatermarkDiffMillis = duration.toMillis)

    /**
      * Sets the partition grouping, can be used to route records to different files.
      */
    def partitionGrouping(grouping: TopicPartition => String): Builder[R, F] = copy(_partitionGrouping = grouping)

    def build(): FileSink[R, F] = {
      if (_fileStorage == null) throw new IllegalStateException("Must specify a FileStorage")
      if (_fileBuilderFactory == null) throw new IllegalStateException("Must specify a FileBuilderFactory")
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")

      new FileSink(
        _fileStorage,
        _fileBuilderFactory,
        _recordFormatter,
        _partitionGrouping,
        _fileCommitStrategy,
        _fileCommitQueueSize,
        _validWatermarkDiffMillis
      )
    }
  }

  def builder[R, F](): Builder[R, F] = Builder[R, F](
    _fileStorage = null,
    _fileBuilderFactory = null,
    _recordFormatter = null,
    _partitionGrouping = _ => "root",
    _fileCommitStrategy = FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(1000)),
    _fileCommitQueueSize = 1,
    _validWatermarkDiffMillis = 3600000
  )
}
