/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import com.adform.streamloader.clickhouse._
import com.adform.streamloader.clickhouse.rowbinary.RowBinaryClickHouseFileBuilder
import com.adform.streamloader.model.{ExampleMessage, Timestamp}
import com.adform.streamloader.sink.batch.{RecordBatchingSink, RecordFormatter}
import com.adform.streamloader.sink.encoding.macros.DataTypeEncodingAnnotation.DecimalEncoding
import com.adform.streamloader.sink.file.Compression
import com.adform.streamloader.sink.file.FileCommitStrategy.ReachedAnyOf
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.{Loader, StreamLoader}
import com.clickhouse.client.api.Client
import com.typesafe.config.ConfigFactory

import java.time.LocalDateTime
import java.util.UUID

/*
CREATE TABLE IF NOT EXISTS test_table (
  _topic String,
  _partition UInt16,
  _offset UInt64,
  _watermark Timestamp,
  id Int32,
  name String,
  timestamp Timestamp,
  height Float64,
  width Float32,
  is_enabled UInt8,
  child_ids Array(Int32),
  parent_id Nullable(Int64),
  transaction_id UUID,
  money_spent Decimal(18, 6)
)
ENGINE = MergeTree()
ORDER BY (_topic, _partition, _offset);
 */

case class TestClickHouseRecord(
    _topic: String,
    _partition: Short,
    _offset: Long,
    _watermark: Timestamp,
    id: Int,
    name: String,
    timestamp: LocalDateTime,
    height: Double,
    width: Float,
    is_enabled: Boolean,
    child_ids: Array[Int],
    parent_id: Option[Long],
    transaction_id: UUID,
    money_spent: BigDecimal @DecimalEncoding(18, 6)
)

object TestClickHouseLoader extends Loader {

  def main(args: Array[String]): Unit = {

    val cfg = ConfigFactory.load().getConfig("stream-loader")
    val client = new Client.Builder()
      .addEndpoint(s"http://${cfg.getString("clickhouse.host")}:${cfg.getInt("clickhouse.port")}")
      .setDefaultDatabase(cfg.getString("clickhouse.db"))
      .setUsername(cfg.getString("clickhouse.user"))
      .setPassword(cfg.getString("clickhouse.password"))
      .build()

    val recordFormatter: RecordFormatter[TestClickHouseRecord] = record => {
      val msg = ExampleMessage.parseFrom(record.consumerRecord.value())
      Seq(
        TestClickHouseRecord(
          record.consumerRecord.topic(),
          record.consumerRecord.partition().toShort,
          record.consumerRecord.offset(),
          record.watermark,
          msg.id,
          msg.name,
          msg.timestamp,
          msg.height,
          msg.width,
          msg.isEnabled,
          msg.childIds,
          msg.parentId,
          msg.transactionId,
          msg.moneySpent
        )
      )
    }

    val source = KafkaSource
      .builder()
      .consumerProperties(cfg.getConfig("kafka.consumer").toProperties)
      .pollTimeout(cfg.getDuration("kafka.poll-timeout"))
      .topics(Seq(cfg.getString("kafka.topic")))
      .build()

    val sink = RecordBatchingSink
      .builder()
      .recordBatcher(
        ClickHouseFileRecordBatcher
          .builder()
          .recordFormatter(recordFormatter)
          .fileBuilderFactory(() => new RowBinaryClickHouseFileBuilder(Compression.ZSTD))
          .fileCommitStrategy(ReachedAnyOf(recordsWritten = Some(cfg.getLong("file.max.records"))))
          .build()
      )
      .batchStorage(
        ClickHouseFileStorage
          .builder()
          .client(client)
          .table(cfg.getString("clickhouse.table"))
          .build()
      )
      .build()

    val loader = new StreamLoader(source, sink)

    sys.addShutdownHook {
      loader.stop()
      client.close()
    }

    loader.start()
  }
}
