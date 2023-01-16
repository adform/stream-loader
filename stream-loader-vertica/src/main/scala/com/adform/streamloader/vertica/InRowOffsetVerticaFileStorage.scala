/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import java.sql.{Connection, SQLDataException}
import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.sink.batch.storage.InDataOffsetBatchStorage
import com.adform.streamloader.util.Logging

import javax.sql.DataSource
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.util.Using

/**
  * A Vertica storage implementation, stores offsets in rows of data.
  * Queries Vertica upon initialization in order to retrieve committed stream positions.
  *
  * Users should keep in mind that the data usage in the licensing audit is calculated treating everything as a string,
  * thus storing the topic, partition and offset next to each row might be very expensive licensing-wise.
  * For a cheaper alternative see the [[ExternalOffsetVerticaFileStorage]].
  */
class InRowOffsetVerticaFileStorage(
    dbDataSource: DataSource,
    table: String,
    topicColumnName: String,
    partitionColumnName: String,
    offsetColumnName: String,
    watermarkColumnName: String
) extends InDataOffsetBatchStorage[InRowOffsetVerticaFileRecordBatch]
    with Logging {

  def committedPositions(connection: Connection): Map[TopicPartition, StreamPosition] = {
    val positionQuery =
      s"""SELECT
         |  $topicColumnName,
         |  $partitionColumnName,
         |  MAX($offsetColumnName) + 1,
         |  MAX($watermarkColumnName)
         |FROM $table
         |WHERE $topicColumnName IS NOT NULL AND $partitionColumnName IS NOT NULL
         |GROUP BY $topicColumnName, $partitionColumnName
         |""".stripMargin

    Using.resource(connection.prepareStatement(positionQuery)) { statement =>
      {
        log.debug(s"Running stream position query: $positionQuery")
        Using.resource(statement.executeQuery()) { result =>
          val positions: mutable.HashMap[TopicPartition, StreamPosition] = mutable.HashMap.empty
          while (result.next()) {
            if (!result.wasNull()) {
              val topicPartition = new TopicPartition(result.getString(1), result.getInt(2))
              val position = StreamPosition(result.getLong(3), Timestamp(result.getTimestamp(4).getTime))
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

  override def commitBatchWithOffsets(batch: InRowOffsetVerticaFileRecordBatch): Unit = {
    Using.resource(dbDataSource.getConnection) { connection =>
      connection.setAutoCommit(false)
      val copyQuery = batch.copyStatement(table)
      Using.resource(connection.prepareStatement(copyQuery)) { copyStatement =>
        try {
          log.info(s"Running statement: $copyQuery")
          val result = copyStatement.executeUpdate()
          connection.commit()
          log.info(s"Successfully committed $result record(s)")
        } catch {
          case e: SQLDataException =>
            log.error(e)("Failed inserting data, rolling back the transaction")
            connection.rollback()
            throw e
        }
      }
    }
  }
}

object InRowOffsetVerticaFileStorage {

  case class Builder(
      private val _dbDataSource: DataSource,
      private val _table: String,
      private val _topicColumnName: String,
      private val _partitionColumnName: String,
      private val _offsetColumnName: String,
      private val _watermarkColumnName: String
  ) {

    /**
      * Sets a data source for Vertica JDBC connections.
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

    def build(): InRowOffsetVerticaFileStorage = {
      if (_dbDataSource == null) throw new IllegalStateException("Must provide a Vertica data source")
      if (_table == null) throw new IllegalStateException("Must provide a valid table name")

      new InRowOffsetVerticaFileStorage(
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
