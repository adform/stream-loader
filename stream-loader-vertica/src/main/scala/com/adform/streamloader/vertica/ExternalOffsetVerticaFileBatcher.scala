/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import java.io.File
import java.time.Duration

import com.adform.streamloader.batch.RecordBatcher
import com.adform.streamloader.file.{BaseFileRecordBatch, FileCommitStrategy}
import com.adform.streamloader.model.{Record, RecordBatchBuilder, RecordRange}
import com.adform.streamloader.util.{Logging, TimeProvider}
import com.adform.streamloader.vertica.file.VerticaFileBuilderFactory
import javax.sql.DataSource

import scala.util.Using

/**
  * A file based Vertica record batch with an extra pre-generated file ID that is used as a foreign key
  * when storing the records and offsets to separate tables.
  */
case class ExternalOffsetVerticaFileRecordBatch(
    file: File,
    fileId: Long,
    recordRanges: Seq[RecordRange],
    copyStatementTemplate: String
) extends BaseFileRecordBatch
    with VerticaRecordBatch {
  override def copyStatement(table: String): String = String.format(copyStatementTemplate, table)
}

/**
  * A file based Vertica record batcher that generates a new file ID before starting a new batch from
  * a given ID sequence, formats records using a given formatter and writes them to files.
  *
  * A `SEQUENCE` is required for generating the `_file_id` foreign key values, create it as follows:
  *
  * {{{
  *   CREATE SEQUENCE file_id_sequence;
  * }}}
  *
  * @param dbDataSource The data source to use when generating file IDs.
  * @param fileIdSequence The file ID sequence name.
  * @param recordFormatter Record formatter to use when writing records to files.
  * @param fileBuilderFactory File builder factory to use.
  * @param fileCommitStrategy Strategy for completing files.
  * @param verticaLoadMethod Vertica load method to use when forming `COPY` statements.
  *
  * @tparam R Type of records written to files.
  */
class ExternalOffsetVerticaFileBatcher[R](
    dbDataSource: DataSource,
    fileIdSequence: String,
    recordFormatter: (Long, Record) => Seq[R],
    fileBuilderFactory: VerticaFileBuilderFactory[R],
    fileCommitStrategy: FileCommitStrategy,
    verticaLoadMethod: VerticaLoadMethod
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends RecordBatcher[ExternalOffsetVerticaFileRecordBatch]
    with Logging {

  override def newBatchBuilder(): RecordBatchBuilder[ExternalOffsetVerticaFileRecordBatch] = {

    val fileId = newFileId
    val fileBuilder = fileBuilderFactory.newFileBuilder()
    val fileStartTimeMillis = timeProvider.currentMillis

    new RecordBatchBuilder[ExternalOffsetVerticaFileRecordBatch] {

      override def add(record: Record): Unit = {
        super.add(record)
        recordFormatter(fileId, record)
          .foreach(formatted => fileBuilder.write(formatted))
      }

      override def isBatchReady: Boolean = fileCommitStrategy.shouldCommit(
        Duration.ofMillis(timeProvider.currentMillis - fileStartTimeMillis),
        fileBuilder.getDataSize,
        fileBuilder.getRecordCount
      )

      override def build(): Option[ExternalOffsetVerticaFileRecordBatch] =
        fileBuilder
          .build()
          .map(
            file =>
              ExternalOffsetVerticaFileRecordBatch(
                file,
                fileId,
                currentRecordRanges,
                fileBuilderFactory.copyStatement(file, "%s", verticaLoadMethod)))

      override def discard(): Unit = fileBuilder.discard()
    }
  }

  private def newFileId: Long = {
    Using.resource(dbDataSource.getConnection) { connection =>
      val query = s"SELECT NEXTVAL('$fileIdSequence')"
      log.info(s"Running stream position query: $query")
      Using.resource(connection.prepareStatement(query)) { statement =>
        Using.resource(statement.executeQuery()) { result =>
          result.next()
          result.getLong(1)
        }
      }
    }
  }
}

object ExternalOffsetVerticaFileBatcher {

  case class Builder[R](
      private val _dbDataSource: DataSource,
      private val _fileIdSequence: String,
      private val _fileBuilderFactory: VerticaFileBuilderFactory[R],
      private val _recordFormatter: (Long, Record) => Seq[R],
      private val _fileCommitStrategy: FileCommitStrategy,
      private val _verticaLoadMethod: VerticaLoadMethod
  ) {

    /**
      * Sets a data source for Vertica JDBC connections.
      */
    def dbDataSource(source: DataSource): Builder[R] = copy(_dbDataSource = source)

    /**
      * Sets the name of the sequence used for generating file IDs.
      */
    def fileIdSequence(name: String): Builder[R] = copy(_fileIdSequence = name)

    /**
      * Sets the load method to use when issuing `COPY` statements.
      */
    def verticaLoadMethod(method: VerticaLoadMethod): Builder[R] = copy(_verticaLoadMethod = method)

    /**
      * Sets the record formatter that converts from consumer records to records written to the file.
      */
    def recordFormatter(formatter: (Long, Record) => Seq[R]): Builder[R] = copy(_recordFormatter = formatter)

    /**
      * Sets the file builder factory, e.g. Native.
      */
    def fileBuilderFactory(factory: VerticaFileBuilderFactory[R]): Builder[R] = copy(_fileBuilderFactory = factory)

    /**
      * Sets the strategy for determining if a file is ready.
      */
    def fileCommitStrategy(strategy: FileCommitStrategy): Builder[R] = copy(_fileCommitStrategy = strategy)

    def build(): ExternalOffsetVerticaFileBatcher[R] = {
      if (_dbDataSource == null) throw new IllegalStateException("Must provide a Vertica data source")
      if (_fileIdSequence == null) throw new IllegalStateException("Must provide a valid file ID sequence name")
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")
      if (_fileBuilderFactory == null) throw new IllegalStateException("Must specify a FileBuilderFactory")

      new ExternalOffsetVerticaFileBatcher(
        _dbDataSource,
        _fileIdSequence,
        _recordFormatter,
        _fileBuilderFactory,
        _fileCommitStrategy,
        _verticaLoadMethod
      )
    }
  }

  def builder[R](): Builder[R] = Builder[R](
    _dbDataSource = null,
    _fileIdSequence = null,
    _fileBuilderFactory = null,
    _recordFormatter = null,
    _fileCommitStrategy = FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(1000)),
    _verticaLoadMethod = VerticaLoadMethod.AUTO
  )
}
