/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse

import com.adform.streamloader.model._
import com.adform.streamloader.sink.batch.storage.InDataOffsetBatchStorage
import com.adform.streamloader.sink.file.Compression
import com.adform.streamloader.util.Logging
import com.clickhouse.client.api.Client
import com.clickhouse.client.api.insert.InsertSettings
import org.apache.kafka.common.TopicPartition

import java.nio.file.Files
import java.time.ZoneOffset
import scala.collection.mutable

/**
  * A ClickHouse storage implementation, stores offsets in rows of data.
  * Queries ClickHouse upon initialization in order to retrieve committed stream positions.
  */
class ClickHouseFileStorage(
    client: Client,
    table: String,
    topicColumnName: String,
    partitionColumnName: String,
    offsetColumnName: String,
    watermarkColumnName: String
) extends InDataOffsetBatchStorage[ClickHouseFileRecordBatch]
    with Logging {

  override def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    val positionQuery =
      s"""SELECT
         |  $topicColumnName,
         |  $partitionColumnName,
         |  MAX($offsetColumnName) + 1,
         |  MAX($watermarkColumnName)
         |FROM $table
         |WHERE isNotNull($topicColumnName) AND isNotNull($partitionColumnName)
         |GROUP BY $topicColumnName, $partitionColumnName
         |""".stripMargin

    log.info(s"Running stream position query: $positionQuery")
    val positions: mutable.HashMap[TopicPartition, StreamPosition] = mutable.HashMap.empty
    client
      .queryAll(positionQuery)
      .forEach(row => {
        val topic = row.getString(1)
        val partition = row.getInteger(2)
        val offset = row.getLong(3)
        val watermark = Timestamp(row.getLocalDateTime(4).toInstant(ZoneOffset.UTC).toEpochMilli)

        val topicPartition = new TopicPartition(topic, partition)
        val position = StreamPosition(offset, watermark)
        positions.put(topicPartition, position)
      })

    topicPartitions.map(tp => (tp, positions.get(tp))).toMap
  }

  override def commitBatchWithOffsets(batch: ClickHouseFileRecordBatch): Unit = {
    val settings = new InsertSettings()
      .setOption("max_insert_block_size", batch.rowCount) // ensure single block to prevent partial writes
      .setDeduplicationToken(deduplicationToken(batch.recordRanges)) // deduplicate based on ranges

    contentEncoding(batch.fileCompression).foreach(encoding => settings.appCompressedData(true, encoding))

    client.insert(table, Files.newInputStream(batch.file.toPath), batch.format, settings).get()
  }

  private def contentEncoding(fileCompression: Compression): Option[String] = fileCompression match {
    case Compression.NONE => None
    case Compression.ZSTD => Some("zstd")
    case Compression.GZIP => Some("gzip")
    case Compression.BZIP => Some("bz2")
    case Compression.LZ4 => Some("lz4")
    case _ => throw new UnsupportedOperationException(s"Compression $fileCompression is not supported by ClickHouse")
  }

  private def deduplicationToken(ranges: Seq[StreamRange]): String = {
    ranges.map(range => s"${range.topic}:${range.partition}:${range.start.offset}:${range.end.offset}").mkString(";")
  }
}

object ClickHouseFileStorage {

  case class Builder(
      private val _client: Client,
      private val _table: String,
      private val _topicColumnName: String,
      private val _partitionColumnName: String,
      private val _offsetColumnName: String,
      private val _watermarkColumnName: String
  ) {

    /**
      * Sets the ClickHouse client.
      */
    def client(client: Client): Builder = copy(_client = client)

    /**
      * Sets the table to load data to.
      */
    def table(name: String): Builder = copy(_table = name)

    /**
      * Sets the names of the columns in the table that are used for storing the stream position
      * this row was producer from. Used in the initialization query that determines committed stream positions.
      */
    def rowOffsetColumnNames(
        topicColumnName: String = "_topic",
        partitionColumnName: String = "_partition",
        offsetColumnName: String = "_offset",
        watermarkColumnName: String = "_watermark"
    ): Builder =
      copy(
        _topicColumnName = topicColumnName,
        _partitionColumnName = partitionColumnName,
        _offsetColumnName = offsetColumnName,
        _watermarkColumnName = watermarkColumnName
      )

    def build(): ClickHouseFileStorage = {
      if (_client == null) throw new IllegalStateException("Must provide a ClickHouse client")
      if (_table == null) throw new IllegalStateException("Must provide a valid table name")

      new ClickHouseFileStorage(
        _client,
        _table,
        _topicColumnName,
        _partitionColumnName,
        _offsetColumnName,
        _watermarkColumnName
      )
    }
  }

  def builder(): Builder = Builder(null, null, "_topic", "_partition", "_offset", "_watermark")
}
