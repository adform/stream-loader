/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import java.time.LocalDateTime
import java.util.UUID

import com.adform.streamloader.batch.{RecordBatchingSink, RecordFormatter}
import com.adform.streamloader.encoding.macros.DataTypeEncodingAnnotation.{DecimalEncoding, MaxLength}
import com.adform.streamloader.file.FileCommitStrategy.ReachedAnyOf
import com.adform.streamloader.file._
import com.adform.streamloader.model.{ExampleMessage, StreamRecord, Timestamp}
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.vertica._
import com.adform.streamloader.vertica.file.native.NativeVerticaFileBuilder
import com.adform.streamloader.{KafkaSource, Loader, Sink, StreamLoader}
import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

abstract class BaseVerticaLoader extends Loader {

  def sink(cfg: Config, dataSource: HikariDataSource): Sink

  def main(args: Array[String]): Unit = {

    val cfg = ConfigFactory.load().getConfig("stream-loader")

    val hikariConf = new HikariConfig()
    hikariConf.setDataSourceClassName(classOf[com.vertica.jdbc.DataSource].getName)

    hikariConf.addDataSourceProperty("host", cfg.getString("vertica.host"))
    hikariConf.addDataSourceProperty("port", cfg.getInt("vertica.port").asInstanceOf[Short])
    hikariConf.addDataSourceProperty("database", cfg.getString("vertica.db"))
    hikariConf.addDataSourceProperty("userID", cfg.getString("vertica.user"))
    hikariConf.addDataSourceProperty("password", cfg.getString("vertica.password"))
    hikariConf.setAutoCommit(false)

    val verticaDataSource = new HikariDataSource(hikariConf)

    val source = KafkaSource
      .builder()
      .consumerProperties(cfg.getConfig("kafka.consumer").toProperties)
      .pollTimeout(cfg.getDuration("kafka.poll-timeout"))
      .topics(Seq(cfg.getString("kafka.topic")))
      .build()

    val loader = new StreamLoader(source, sink(cfg, verticaDataSource))

    sys.addShutdownHook {
      loader.stop()
      verticaDataSource.close()
    }

    loader.start()
  }
}

/*
CREATE SEQUENCE file_id_sequence;

CREATE TABLE file_offsets (
  _file_id INT NOT NULL,
  _consumer_group VARCHAR(1024) NOT NULL,
  _topic VARCHAR(128) NOT NULL,
  _partition INT NOT NULL,
  _start_offset INT NOT NULL,
  _start_watermark TIMESTAMP NOT NULL,
  _end_offset INT NOT NULL,
  _end_watermark TIMESTAMP NOT NULL
);

CREATE TABLE test_table (
  _file_id INT NOT NULL,
  id INT NOT NULL,
  name VARCHAR(500) NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  height FLOAT NOT NULL,
  width FLOAT NOT NULL,
  is_enabled BOOLEAN NOT NULL,
  child_ids VARCHAR(64000) NOT NULL,
  parent_id INT,
  transaction_id UUID NOT NULL,
  money_spent DECIMAL(18, 6) NOT NULL
);
 */

case class TestExternalOffsetVerticaRecord(
    _file_id: Long,
    id: Int,
    name: String @MaxLength(500),
    timestamp: LocalDateTime,
    height: Double,
    width: Float,
    is_enabled: Boolean,
    child_ids: String,
    parent_id: Option[Long],
    transaction_id: UUID,
    money_spent: BigDecimal @DecimalEncoding(18, 6)
)

object TestExternalOffsetVerticaLoader extends BaseVerticaLoader {

  private val recordFormatter = (fileId: Long, record: StreamRecord) => {
    val msg = ExampleMessage.parseFrom(record.consumerRecord.value())
    Seq(
      TestExternalOffsetVerticaRecord(
        fileId,
        msg.id,
        msg.name,
        msg.timestamp,
        msg.height,
        msg.width,
        msg.isEnabled,
        msg.childIds.mkString(";"),
        msg.parentId,
        msg.transactionId,
        msg.moneySpent
      ))
  }

  override def sink(cfg: Config, verticaDataSource: HikariDataSource): Sink =
    RecordBatchingSink
      .builder()
      .recordBatcher(
        ExternalOffsetVerticaFileBatcher
          .builder()
          .dbDataSource(verticaDataSource)
          .fileIdSequence(cfg.getString("vertica.file-id-sequence"))
          .recordFormatter(recordFormatter)
          .fileBuilderFactory(() => new NativeVerticaFileBuilder(Compression.ZSTD))
          .fileCommitStrategy(ReachedAnyOf(recordsWritten = Some(cfg.getLong("file.max.records"))))
          .verticaLoadMethod(VerticaLoadMethod.AUTO)
          .build()
      )
      .batchStorage(
        ExternalOffsetVerticaFileStorage
          .builder()
          .dbDataSource(verticaDataSource)
          .table(cfg.getString("vertica.table"))
          .offsetTable(cfg.getString("vertica.offset-table"))
          .build()
      )
      .build()
}

/*
CREATE TABLE test_table(
  _topic VARCHAR(128) NOT NULL,
  _partition INT NOT NULL,
  _offset INT NOT NULL,
  _watermark TIMESTAMP NOT NULL,
  id INT NOT NULL,
  name VARCHAR(500) NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  height FLOAT NOT NULL,
  width FLOAT NOT NULL,
  is_enabled BOOLEAN NOT NULL,
  child_ids VARCHAR(64000) NOT NULL,
  parent_id INT,
  transaction_id UUID NOT NULL,
  money_spent DECIMAL(18, 6) NOT NULL
);
 */

case class TestInRowOffsetVerticaRecord(
    _topic: String,
    _partition: Int,
    _offset: Long,
    _watermark: Timestamp,
    id: Int,
    name: String @MaxLength(500),
    timestamp: LocalDateTime,
    height: Double,
    width: Float,
    is_enabled: Boolean,
    child_ids: String,
    parent_id: Option[Long],
    transaction_id: UUID,
    money_spent: BigDecimal @DecimalEncoding(18, 6)
)

object TestInRowOffsetVerticaLoader extends BaseVerticaLoader {

  private val recordFormatter: RecordFormatter[TestInRowOffsetVerticaRecord] = record => {
    val msg = ExampleMessage.parseFrom(record.consumerRecord.value())
    Seq(
      TestInRowOffsetVerticaRecord(
        record.consumerRecord.topic(),
        record.consumerRecord.partition(),
        record.consumerRecord.offset(),
        record.watermark,
        msg.id,
        msg.name,
        msg.timestamp,
        msg.height,
        msg.width,
        msg.isEnabled,
        msg.childIds.mkString(";"),
        msg.parentId,
        msg.transactionId,
        msg.moneySpent
      )
    )
  }

  override def sink(cfg: Config, verticaDataSource: HikariDataSource): Sink =
    RecordBatchingSink
      .builder()
      .recordBatcher(
        InRowOffsetVerticaFileRecordBatcher
          .builder()
          .recordFormatter(recordFormatter)
          .fileBuilderFactory(() => new NativeVerticaFileBuilder(Compression.ZSTD))
          .fileCommitStrategy(ReachedAnyOf(recordsWritten = Some(cfg.getLong("file.max.records"))))
          .verticaLoadMethod(VerticaLoadMethod.AUTO)
          .build()
      )
      .batchStorage(
        InRowOffsetVerticaFileStorage
          .builder()
          .dbDataSource(verticaDataSource)
          .table(cfg.getString("vertica.table"))
          .build()
      )
      .build()
}
