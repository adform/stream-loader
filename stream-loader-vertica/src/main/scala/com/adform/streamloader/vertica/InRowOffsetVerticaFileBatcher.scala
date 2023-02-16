/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import java.io.File
import com.adform.streamloader.model.StreamRange
import com.adform.streamloader.sink.batch.RecordFormatter
import com.adform.streamloader.sink.file.{FileCommitStrategy, FileRecordBatch, FileRecordBatcher}
import com.adform.streamloader.vertica.file.{VerticaFileBuilder, VerticaFileBuilderFactory}

case class InRowOffsetVerticaFileRecordBatch(
    file: File,
    recordRanges: Seq[StreamRange],
    copyStatementTemplate: String
) extends FileRecordBatch
    with VerticaRecordBatch {
  override def copyStatement(table: String): String = String.format(copyStatementTemplate, table)
}

/**
  * A record batcher that passes records through a custom record formatter and forms batches by writing
  * the resulting records to files using a provided file builder.
  *
  * @tparam R Type of records being written to files.
  */
class InRowOffsetVerticaFileRecordBatcher[R](
    recordFormatter: RecordFormatter[R],
    fileBuilderFactory: VerticaFileBuilderFactory[R],
    fileCommitStrategy: FileCommitStrategy,
    verticaLoadMethod: VerticaLoadMethod
) extends FileRecordBatcher[R, InRowOffsetVerticaFileRecordBatch, VerticaFileBuilder[R]](
      recordFormatter,
      fileBuilderFactory,
      fileCommitStrategy
    ) {

  override def constructBatch(
      fileBuilder: VerticaFileBuilder[R],
      recordRanges: Seq[StreamRange]
  ): Option[InRowOffsetVerticaFileRecordBatch] = {
    fileBuilder
      .build()
      .map(file =>
        InRowOffsetVerticaFileRecordBatch(file, recordRanges, fileBuilder.copyStatement(file, "%s", verticaLoadMethod))
      )
  }
}

object InRowOffsetVerticaFileRecordBatcher {

  case class Builder[R](
      private val _fileBuilderFactory: VerticaFileBuilderFactory[R],
      private val _recordFormatter: RecordFormatter[R],
      private val _fileCommitStrategy: FileCommitStrategy,
      private val _verticaLoadMethod: VerticaLoadMethod
  ) {

    /**
      * Sets the load method to use when issuing `COPY` statements.
      */
    def verticaLoadMethod(method: VerticaLoadMethod): Builder[R] = copy(_verticaLoadMethod = method)

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: RecordFormatter[R]): Builder[R] = copy(_recordFormatter = formatter)

    /**
      * Sets the file builder factory, e.g. Native.
      */
    def fileBuilderFactory(factory: VerticaFileBuilderFactory[R]): Builder[R] = copy(_fileBuilderFactory = factory)

    /**
      * Sets the strategy for determining if a file is ready.
      */
    def fileCommitStrategy(strategy: FileCommitStrategy): Builder[R] = copy(_fileCommitStrategy = strategy)

    def build(): InRowOffsetVerticaFileRecordBatcher[R] = {
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")
      if (_fileBuilderFactory == null) throw new IllegalStateException("Must specify a FileBuilderFactory")

      new InRowOffsetVerticaFileRecordBatcher(
        _recordFormatter,
        _fileBuilderFactory,
        _fileCommitStrategy,
        _verticaLoadMethod
      )
    }
  }

  def builder[R](): Builder[R] = Builder[R](
    _fileBuilderFactory = null,
    _recordFormatter = null,
    _fileCommitStrategy = FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(1000)),
    _verticaLoadMethod = VerticaLoadMethod.AUTO
  )
}
