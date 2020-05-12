/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse

import java.sql.Connection

import com.adform.streamloader.file.RecordRangeFile
import com.adform.streamloader.file.storage.InDataOffsetFileStorage
import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.Logging
import javax.sql.DataSource
import org.apache.kafka.common.TopicPartition
import ru.yandex.clickhouse.ClickHouseConnection
import ru.yandex.clickhouse.domain.ClickHouseFormat
import ru.yandex.clickhouse.settings.ClickHouseQueryParam

import scala.collection.concurrent.TrieMap
import scala.util.Using

/**
  * A ClickHouse storage implementation, stores offsets in rows of data.
  * Queries ClickHouse upon initialization in order to retrieve committed stream positions.
  */
class ClickHouseFileStorage private (
    dbDataSource: DataSource,
    table: String,
    fileFormat: ClickHouseFormat,
    topicColumnName: String,
    partitionColumnName: String,
    offsetColumnName: String,
    watermarkColumnName: String
) extends InDataOffsetFileStorage[Unit]
    with Logging {

  override def startNewFile(): Unit = {}

  def committedPositions(connection: Connection): TrieMap[TopicPartition, StreamPosition] = {
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

    Using.resource(connection.prepareStatement(positionQuery)) { statement =>
      {
        log.info(s"Running stream position query: $positionQuery")
        Using.resource(statement.executeQuery()) { result =>
          val positions: TrieMap[TopicPartition, StreamPosition] = TrieMap.empty
          while (result.next()) {
            val topic = result.getString(1)
            val partition = result.getInt(2)
            val offset = result.getLong(3)
            val watermark = Timestamp(result.getTimestamp(4).getTime)
            if (!result.wasNull()) {
              val topicPartition = new TopicPartition(topic, partition)
              val position = StreamPosition(offset, watermark)
              positions.put(topicPartition, position)
            }
          }
          positions
        }
      }
    }
  }

  override def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    Using.resource(dbDataSource.getConnection()) { connection =>
      val positions = committedPositions(connection)
      topicPartitions.map(tp => (tp, positions.get(tp))).toMap
    }
  }

  override def storeFile(file: RecordRangeFile[Unit]): Unit = {
    Using.resource(dbDataSource.getConnection) { connection =>
      Using.resource(connection.unwrap(classOf[ClickHouseConnection]).createStatement) { statement =>
        statement
          .write()
          .table(table)
          .data(file.file, fileFormat)
          .addDbParam(ClickHouseQueryParam.MAX_INSERT_BLOCK_SIZE, file.recordCount.toString) // atomic insert
          .send()
      }
    }
  }
}

object ClickHouseFileStorage {

  case class Builder(
      private val _dbDataSource: DataSource,
      private val _table: String,
      private val _fileFormat: ClickHouseFormat,
      private val _topicColumnName: String,
      private val _partitionColumnName: String,
      private val _offsetColumnName: String,
      private val _watermarkColumnName: String) {

    /**
      * Sets a data source for ClickHouse JDBC connections.
      */
    def dbDataSource(source: DataSource): Builder = copy(_dbDataSource = source)

    /**
      * Sets the table to load data to.
      */
    def table(name: String): Builder = copy(_table = name)

    /**
      * Sets the file format that is being used.
      */
    def fileFormat(format: ClickHouseFormat): Builder = copy(_fileFormat = format)

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
      if (_dbDataSource == null) throw new IllegalStateException("Must provide a ClickHouse data source")
      if (_table == null) throw new IllegalStateException("Must provide a valid table name")
      if (_fileFormat == null) throw new IllegalStateException("Must provide the file format")

      new ClickHouseFileStorage(
        _dbDataSource,
        _table,
        _fileFormat,
        _topicColumnName,
        _partitionColumnName,
        _offsetColumnName,
        _watermarkColumnName)
    }
  }

  def builder(): Builder = Builder(null, null, null, "_topic", "_partition", "_offset", "_watermark")
}
