/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.iceberg

import com.adform.streamloader.model.{StreamRange, StreamRecord}
import com.adform.streamloader.sink.batch.{RecordBatch, RecordBatchBuilder, RecordBatcher, RecordFormatter}
import com.adform.streamloader.sink.file.{FileStats, MultiFileCommitStrategy}
import com.adform.streamloader.util.TimeProvider
import com.adform.streamloader.util.UuidExtensions.randomUUIDv7
import org.apache.iceberg.data.{GenericAppenderFactory, Record => IcebergRecord}
import org.apache.iceberg.io.DataWriteResult
import org.apache.iceberg.{FileFormat, PartitionKey, Table}

import java.time.Duration
import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class IcebergRecordBatch(dataWriteResult: DataWriteResult, recordRanges: Seq[StreamRange]) extends RecordBatch {
  override def discard(): Boolean = true
}

/**
  * Record batch builder that collects records to files per partition.
  */
class IcebergRecordBatchBuilder(
    table: Table,
    recordFormatter: RecordFormatter[IcebergRecord],
    fileFormat: FileFormat,
    fileCommitStrategy: MultiFileCommitStrategy,
    writeProperties: Map[String, String]
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends RecordBatchBuilder[IcebergRecordBatch] {

  private class PartitionDataWriter(pk: PartitionKey) {
    private val startTimeMillis = timeProvider.currentMillis
    private var recordCount = 0L
    private val dataWriter = {
      val filename = fileFormat.addExtension(randomUUIDv7().toString)
      val path = table.locationProvider().newDataLocation(table.spec(), pk, filename)
      val output = table.io().newOutputFile(path)

      val factory = new GenericAppenderFactory(table.schema(), table.spec())
      factory.setAll(writeProperties.asJava)

      val encrypted = table.encryption().encrypt(output)
      factory.newDataWriter(encrypted, fileFormat, pk)
    }

    def write(record: IcebergRecord): Unit = {
      dataWriter.write(record)
      recordCount += 1
    }

    def build(): DataWriteResult = {
      dataWriter.close()
      dataWriter.result()
    }

    def fileStats: FileStats = FileStats(
      Duration.ofMillis(timeProvider.currentMillis - startTimeMillis),
      dataWriter.length(),
      recordCount
    )
  }

  private val partitionWriters: mutable.HashMap[PartitionKey, PartitionDataWriter] = mutable.HashMap.empty

  override protected def addToBatch(record: StreamRecord): Int = {
    var cnt = 0
    recordFormatter
      .format(record)
      .foreach(record => {
        val pk = new PartitionKey(table.spec(), table.schema())
        pk.partition(record)

        val writer = partitionWriters.getOrElseUpdate(pk, new PartitionDataWriter(pk))
        writer.write(record)

        cnt += 1
      })
    cnt
  }

  override def isBatchReady: Boolean = {
    fileCommitStrategy.shouldCommit(partitionWriters.map(kv => kv._2.fileStats).toSeq)
  }

  override def build(): Option[IcebergRecordBatch] = {
    val files = partitionWriters.map(kv => kv._2.build()).flatMap(_.dataFiles().asScala)
    if (files.nonEmpty) {
      Some(IcebergRecordBatch(new DataWriteResult(files.toList.asJava), currentRecordRanges))
    } else {
      None
    }
  }

  override def discard(): Unit = partitionWriters.foreach(_._2.build())
}

class IcebergRecordBatcher(
    table: Table,
    recordFormatter: RecordFormatter[IcebergRecord],
    fileFormat: FileFormat,
    fileCommitStrategy: MultiFileCommitStrategy,
    writeProperties: Map[String, String]
) extends RecordBatcher[IcebergRecordBatch] {

  override def newBatchBuilder(): RecordBatchBuilder[IcebergRecordBatch] = new IcebergRecordBatchBuilder(
    table,
    recordFormatter,
    fileFormat,
    fileCommitStrategy,
    writeProperties
  )
}

object IcebergRecordBatcher {

  case class Builder(
      private val _table: Table,
      private val _recordFormatter: RecordFormatter[IcebergRecord],
      private val _fileFormat: FileFormat,
      private val _fileCommitStrategy: MultiFileCommitStrategy,
      private val _writeProperties: Map[String, String]
  ) {

    /**
      * Sets the Iceberg table to build batches for.
      */
    def table(table: Table): Builder = copy(_table = table)

    /**
      * Sets the record formatter that converts from consumer records to Iceberg records.
      */
    def recordFormatter(formatter: RecordFormatter[IcebergRecord]): Builder = copy(_recordFormatter = formatter)

    /**
      * Sets the file format to use.
      */
    def fileFormat(format: FileFormat): Builder = copy(_fileFormat = format)

    /**
      * Sets the strategy for determining if a batch of files (one per partition) is ready to be stored.
      */
    def fileCommitStrategy(strategy: MultiFileCommitStrategy): Builder = copy(_fileCommitStrategy = strategy)

    /**
      * Sets any additional properties for the underlying data file builder.
      */
    def writeProperties(properties: Map[String, String]): Builder = copy(_writeProperties = properties)

    def build(): IcebergRecordBatcher = {
      if (_table == null) throw new IllegalStateException("Must specify a destination table")
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")
      if (_fileCommitStrategy == null) throw new IllegalStateException("Must specify a FileCommitStrategy")

      new IcebergRecordBatcher(_table, _recordFormatter, _fileFormat, _fileCommitStrategy, _writeProperties)
    }
  }

  def builder(): Builder = Builder(
    _table = null,
    _recordFormatter = null,
    _fileFormat = FileFormat.PARQUET,
    _fileCommitStrategy = null,
    _writeProperties = Map.empty
  )
}
