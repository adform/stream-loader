/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.io.File
import java.time.Duration

import com.adform.streamloader.batch.{RecordBatcher, RecordFormatter}
import com.adform.streamloader.model.{Record, RecordBatchBuilder, RecordRange}
import com.adform.streamloader.util.TimeProvider

/**
  * A record batcher that passes records through a custom record formatter and forms batches by writing
  * the formatted records to files using a provided file builder.
  *
  * @param recordFormatter A formatter to use for writing records to files.
  * @param fileBuilderFactory A file builder to use.
  * @param fileCommitStrategy Strategy for completing batches.
  *
  * @tparam R Type of records being written to files.
  * @tparam B Type of record batches being built.
  */
abstract class BaseFileRecordBatcher[+R, +B <: BaseFileRecordBatch](
    recordFormatter: RecordFormatter[R],
    fileBuilderFactory: FileBuilderFactory[R],
    fileCommitStrategy: FileCommitStrategy
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends RecordBatcher[B] {

  /**
    * Constructs the final batch of a concrete type given the formed file and the accumulated record statistics.
    */
  def constructBatch(file: File, recordRanges: Seq[RecordRange], recordCount: Long, formattedRecordCount: Long): B

  override def newBatchBuilder(): RecordBatchBuilder[B] = {
    val fileStartTimeMillis = timeProvider.currentMillis
    val fileBuilder = fileBuilderFactory.newFileBuilder()

    new RecordBatchBuilder[B] {

      override def add(record: Record): Unit = {
        super.add(record)
        recordFormatter
          .format(record)
          .foreach(formatted => fileBuilder.write(formatted))
      }

      override def isBatchReady: Boolean = fileCommitStrategy.shouldCommit(
        Duration.ofMillis(timeProvider.currentMillis - fileStartTimeMillis),
        fileBuilder.getDataSize,
        fileBuilder.getRecordCount
      )

      override def build(): Option[B] =
        fileBuilder
          .build()
          .map(file => constructBatch(file, currentRecordRanges, currentRecordCount, fileBuilder.getRecordCount))

      override def discard(): Unit = fileBuilder.discard()
    }
  }
}

/**
  * A record batcher that passes records through a custom record formatter and forms batches by writing
  * the resulting records to files using a provided file builder.
  *
  * @tparam R Type of records being written to files.
  */
class FileRecordBatcher[+R](
    recordFormatter: RecordFormatter[R],
    fileBuilderFactory: FileBuilderFactory[R],
    fileCommitStrategy: FileCommitStrategy
) extends BaseFileRecordBatcher[R, FileRecordBatch](recordFormatter, fileBuilderFactory, fileCommitStrategy) {

  override def constructBatch(
      file: File,
      recordRanges: Seq[RecordRange],
      recordCount: Long,
      formattedRecordCount: Long): FileRecordBatch =
    FileRecordBatch(file, recordRanges)
}

object FileRecordBatcher {

  case class Builder[R](
      private val _fileBuilderFactory: FileBuilderFactory[R],
      private val _recordFormatter: RecordFormatter[R],
      private val _fileCommitStrategy: FileCommitStrategy
  ) {

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: RecordFormatter[R]): Builder[R] = copy(_recordFormatter = formatter)

    /**
      * Sets the file builder factory, e.g. CSV.
      */
    def fileBuilderFactory(factory: FileBuilderFactory[R]): Builder[R] = copy(_fileBuilderFactory = factory)

    /**
      * Sets the strategy for determining if a file is ready.
      */
    def fileCommitStrategy(strategy: FileCommitStrategy): Builder[R] = copy(_fileCommitStrategy = strategy)

    def build(): FileRecordBatcher[R] = {
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")
      if (_fileBuilderFactory == null) throw new IllegalStateException("Must specify a FileBuilderFactory")

      new FileRecordBatcher(
        _recordFormatter,
        _fileBuilderFactory,
        _fileCommitStrategy,
      )
    }
  }

  def builder[R](): Builder[R] = Builder[R](
    _fileBuilderFactory = null,
    _recordFormatter = null,
    _fileCommitStrategy = FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(1000))
  )
}
