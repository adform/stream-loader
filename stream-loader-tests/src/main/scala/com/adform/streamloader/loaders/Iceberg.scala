/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import com.adform.streamloader.iceberg.{IcebergRecordBatchStorage, IcebergRecordBatcher}
import com.adform.streamloader.model.ExampleMessage
import com.adform.streamloader.sink.batch.{RecordBatchingSink, RecordFormatter}
import com.adform.streamloader.sink.file.FileCommitStrategy._
import com.adform.streamloader.sink.file.MultiFileCommitStrategy
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.util.UuidExtensions._
import com.adform.streamloader.{Loader, StreamLoader}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.FileFormat
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.{GenericRecord, Record => IcebergRecord}
import org.apache.iceberg.hadoop.HadoopCatalog

import java.time.ZoneOffset
import java.util
import java.util.concurrent.locks.ReentrantLock

object TestIcebergLoader extends Loader {

  def main(args: Array[String]): Unit = {

    val cfg = ConfigFactory.load().getConfig("stream-loader")

    val catalog = new HadoopCatalog(new Configuration(), cfg.getString("iceberg.warehouse-dir"))
    val table = catalog.loadTable(TableIdentifier.parse(cfg.getString("iceberg.table")))

    val recordFormatter: RecordFormatter[IcebergRecord] = record => {
      val avroMessage = ExampleMessage.parseFrom(record.consumerRecord.value())
      val icebergRecord = GenericRecord.create(table.schema())

      icebergRecord.setField("id", avroMessage.id)
      icebergRecord.setField("name", avroMessage.name)
      icebergRecord.setField("timestamp", avroMessage.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli)
      icebergRecord.setField("height", avroMessage.height)
      icebergRecord.setField("width", avroMessage.width)
      icebergRecord.setField("isEnabled", avroMessage.isEnabled)
      icebergRecord.setField("childIds", util.Arrays.asList(avroMessage.childIds: _*))
      icebergRecord.setField("parentId", avroMessage.parentId.orNull)
      icebergRecord.setField("transactionId", avroMessage.transactionId.toBytes)
      icebergRecord.setField("moneySpent", avroMessage.moneySpent.bigDecimal)

      Seq(icebergRecord)
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
        IcebergRecordBatcher
          .builder()
          .recordFormatter(recordFormatter)
          .table(table)
          .fileFormat(FileFormat.PARQUET)
          .fileCommitStrategy(
            MultiFileCommitStrategy.total(ReachedAnyOf(recordsWritten = Some(cfg.getLong("file.max.records"))))
          )
          .writeProperties(
            Map("write.parquet.compression-codec" -> "zstd")
          )
          .build()
      )
      .batchStorage(
        IcebergRecordBatchStorage
          .builder()
          .table(table)
          .commitLock(new ReentrantLock())
          .build()
      )
      .build()

    val loader = new StreamLoader(source, sink)

    sys.addShutdownHook {
      loader.stop()
      catalog.close()
    }

    loader.start()
  }
}
