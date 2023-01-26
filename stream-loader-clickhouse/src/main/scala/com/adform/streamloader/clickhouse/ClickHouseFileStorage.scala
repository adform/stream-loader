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
import com.adform.streamloader.util.Logging
import com.clickhouse.data.ClickHouseFile
import com.clickhouse.jdbc.ClickHouseConnection
import org.apache.kafka.common.TopicPartition

import java.sql.Connection
import javax.sql.DataSource
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
  * A ClickHouse storage implementation, stores offsets in rows of data.
  * Queries ClickHouse upon initialization in order to retrieve committed stream positions.
  */
class ClickHouseFileStorage(
    dbDataSource: DataSource,
    table: String,
    topicColumnName: String,
    partitionColumnName: String,
    offsetColumnName: String,
    watermarkColumnName: String
) extends InDataOffsetBatchStorage[ClickHouseFileRecordBatch]
    with Logging {

  def committedPositions(connection: Connection): Map[TopicPartition, StreamPosition] = {
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
          val positions: mutable.HashMap[TopicPartition, StreamPosition] = mutable.HashMap.empty
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
          positions.toMap
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

  override def commitBatchWithOffsets(batch: ClickHouseFileRecordBatch): Unit = {
    Using.resource(dbDataSource.getConnection) { connection =>
      Using.resource(connection.unwrap(classOf[ClickHouseConnection]).createStatement) { statement =>
        statement
          .write()
          .data(ClickHouseFile.of(batch.file, batch.compression, 1, batch.format))
          .table(table)
          .params(Map("max_insert_block_size" -> batch.rowCount.toString).asJava) // atomic insert
          .executeAndWait()
      }
    }
  }
}

object ClickHouseFileStorage {

  case class Builder(
      private val _dbDataSource: DataSource,
      private val _table: String,
      private val _topicColumnName: String,
      private val _partitionColumnName: String,
      private val _offsetColumnName: String,
      private val _watermarkColumnName: String
  ) {

    /**
      * Sets a data source for ClickHouse JDBC connections.
      */
    def dbDataSource(source: DataSource): Builder = copy(_dbDataSource = source)

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
      if (_dbDataSource == null) throw new IllegalStateException("Must provide a ClickHouse data source")
      if (_table == null) throw new IllegalStateException("Must provide a valid table name")

      new ClickHouseFileStorage(
        _dbDataSource,
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
