/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse

import com.adform.streamloader.batch.RecordFormatter
import com.adform.streamloader.file.{FileCommitStrategy, FileRecordBatcher}
import com.adform.streamloader.model.StreamRange

class ClickHouseFileRecordBatcher[R](
    recordFormatter: RecordFormatter[R],
    fileBuilderFactory: ClickHouseFileBuilderFactory[R],
    fileCommitStrategy: FileCommitStrategy
) extends FileRecordBatcher[R, ClickHouseFileRecordBatch, ClickHouseFileBuilder[R]](
      recordFormatter,
      fileBuilderFactory,
      fileCommitStrategy
    ) {

  override def constructBatch(
      fileBuilder: ClickHouseFileBuilder[R],
      recordRanges: Seq[StreamRange],
      recordCount: Long
  ): Option[ClickHouseFileRecordBatch] = {
    fileBuilder
      .build()
      .map(f => ClickHouseFileRecordBatch(f, fileBuilder.format, recordRanges, fileBuilder.getRecordCount))
  }
}

object ClickHouseFileRecordBatcher {

  case class Builder[R](
      private val _fileBuilderFactory: ClickHouseFileBuilderFactory[R],
      private val _recordFormatter: RecordFormatter[R],
      private val _fileCommitStrategy: FileCommitStrategy
  ) {

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: RecordFormatter[R]): Builder[R] = copy(_recordFormatter = formatter)

    /**
      * Sets the file builder factory, e.g. RowBinary
      */
    def fileBuilderFactory(factory: ClickHouseFileBuilderFactory[R]): Builder[R] = copy(_fileBuilderFactory = factory)

    /**
      * Sets the strategy for determining if a file is ready.
      */
    def fileCommitStrategy(strategy: FileCommitStrategy): Builder[R] = copy(_fileCommitStrategy = strategy)

    def build(): ClickHouseFileRecordBatcher[R] = {
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")
      if (_fileBuilderFactory == null) throw new IllegalStateException("Must specify a FileBuilderFactory")

      new ClickHouseFileRecordBatcher(
        _recordFormatter,
        _fileBuilderFactory,
        _fileCommitStrategy
      )
    }
  }

  def builder[R](): Builder[R] = Builder[R](
    _fileBuilderFactory = null,
    _recordFormatter = null,
    _fileCommitStrategy = FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(1000))
  )
}
