/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.time.Duration

import com.adform.streamloader.batch.{RecordBatchBuilder, RecordBatcher, RecordFormatter, RecordPartitioner}
import com.adform.streamloader.file.FileCommitStrategy.ReachedAnyOf
import com.adform.streamloader.model.StreamRecord
import com.adform.streamloader.util.TimeProvider

import scala.collection.concurrent.TrieMap

/**
  * A record batcher that distributes records into user defined partitions using a given partitioner
  * and writes them to separate files per partition.
  *
  * @param recordFormatter Record formatter to use when writing to files.
  * @param recordPartitioner Partitioner for distributing records to partitions.
  * @param fileBuilderFactory File builder factory to use to construct files for partitions.
  * @param fileCommitStrategy File commit strategy to use.
  * @tparam P Type of the partition values.
  * @tparam R Type of formatted records.
  */
class PartitioningFileRecordBatcher[P, R](
    recordFormatter: RecordFormatter[R],
    recordPartitioner: RecordPartitioner[R, P],
    fileBuilderFactory: P => FileBuilder[R],
    fileCommitStrategy: MultiFileCommitStrategy
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends RecordBatcher[PartitionedFileRecordBatch[P, SingleFileRecordBatch]] {

  private case class FileRecordBatchBuilder(startTimeMs: Long, fileBuilder: FileBuilder[R])
      extends RecordBatchBuilder[SingleFileRecordBatch] {

    def write(record: StreamRecord, formattedRecord: R): Unit = {
      add(record)
      fileBuilder.write(formattedRecord)
    }
    override def isBatchReady: Boolean = false

    override def build(): Option[SingleFileRecordBatch] =
      fileBuilder.build().map(f => SingleFileRecordBatch(f, currentRecordRanges))

    override def discard(): Unit = fileBuilder.discard()
  }

  override def newBatchBuilder(): RecordBatchBuilder[PartitionedFileRecordBatch[P, SingleFileRecordBatch]] =
    new RecordBatchBuilder[PartitionedFileRecordBatch[P, SingleFileRecordBatch]] {

      private val partitionBuilders: TrieMap[P, FileRecordBatchBuilder] = TrieMap.empty

      override def add(record: StreamRecord): Unit = {
        super.add(record)
        recordFormatter
          .format(record)
          .foreach(formatted => {
            val partition = recordPartitioner.partition(record, formatted)
            val partitionBuilder = partitionBuilders.getOrElseUpdate(
              partition,
              FileRecordBatchBuilder(timeProvider.currentMillis, fileBuilderFactory(partition))
            )
            partitionBuilder.write(record, formatted)
          })
      }

      override def isBatchReady: Boolean = fileCommitStrategy.shouldCommit(
        partitionBuilders.map {
          case (_, partitionBuilder) =>
            FileStats(
              Duration.ofMillis(timeProvider.currentMillis - partitionBuilder.startTimeMs),
              partitionBuilder.fileBuilder.getDataSize,
              partitionBuilder.fileBuilder.getRecordCount
            )
        }.toSeq
      )

      override def build(): Option[PartitionedFileRecordBatch[P, SingleFileRecordBatch]] = {
        val batches = for {
          (partition, builder) <- partitionBuilders
          batch <- builder.build()
        } yield (partition, batch)

        if (batches.nonEmpty)
          Some(PartitionedFileRecordBatch(batches.toMap))
        else
          None
      }

      override def discard(): Unit = partitionBuilders.values.foreach(_.discard())
    }
}

object PartitioningFileRecordBatcher {

  case class Builder[P, R](
      private val _fileBuilderFactory: P => FileBuilder[R],
      private val _recordFormatter: RecordFormatter[R],
      private val _recordPartitioner: RecordPartitioner[R, P],
      private val _fileCommitStrategy: MultiFileCommitStrategy
  ) {

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: RecordFormatter[R]): Builder[P, R] = copy(_recordFormatter = formatter)

    /**
      * Sets the record partitioner to use for distributing records to partitions.
      */
    def recordPartitioner(partitioner: RecordPartitioner[R, P]): Builder[P, R] = copy(_recordPartitioner = partitioner)

    /**
      * Sets the file builder factory to use for constructing files for partitions, e.g. CSV.
      */
    def fileBuilderFactory(factory: P => FileBuilder[R]): Builder[P, R] = copy(_fileBuilderFactory = factory)

    /**
      * Sets the strategy for determining if a batch of files is ready.
      */
    def fileCommitStrategy(strategy: MultiFileCommitStrategy): Builder[P, R] = copy(_fileCommitStrategy = strategy)

    def build(): PartitioningFileRecordBatcher[P, R] = {
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")
      if (_recordPartitioner == null) throw new IllegalStateException("Must specify a RecordPartitioner")
      if (_fileBuilderFactory == null) throw new IllegalStateException("Must specify a FileBuilderFactory")

      new PartitioningFileRecordBatcher(
        _recordFormatter,
        _recordPartitioner,
        _fileBuilderFactory,
        _fileCommitStrategy,
      )
    }
  }

  def builder[P, R](): Builder[P, R] = Builder[P, R](
    _fileBuilderFactory = null,
    _recordFormatter = null,
    _recordPartitioner = null,
    _fileCommitStrategy = MultiFileCommitStrategy.anyFile(ReachedAnyOf(recordsWritten = Some(1000)))
  )
}
