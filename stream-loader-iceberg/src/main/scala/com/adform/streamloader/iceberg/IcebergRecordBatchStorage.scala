/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.iceberg

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.sink.batch.storage.InDataOffsetBatchStorage
import org.apache.iceberg.Table
import org.apache.kafka.common.TopicPartition

/**
  * Iceberg record batch storage that appends multiple files and stores Kafka offsets in table properties
  * in a single atomic table transaction.
  */
class IcebergRecordBatchStorage(table: Table) extends InDataOffsetBatchStorage[IcebergRecordBatch] {

  private def offsetKey(topic: String, partition: Int): String = {
    s"__consumer_offset:${kafkaContext.consumerGroup}:$topic:$partition"
  }

  override def recover(topicPartitions: Set[TopicPartition]): Unit = {}

  override def commitBatchWithOffsets(batch: IcebergRecordBatch): Unit = {
    val transaction = table.newTransaction()

    batch.dataWriteResult.dataFiles().forEach(file => transaction.newAppend().appendFile(file).commit())

    batch.recordRanges.foreach(range => {
      transaction
        .updateProperties()
        .set(offsetKey(range.topic, range.partition), s"${range.end.offset}:${range.end.watermark.millis}")
        .commit()
    })

    transaction.commitTransaction()
    log.info(s"Successfully commited Iceberg transaction for ranges ${batch.recordRanges.mkString(",")}")
  }

  override def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    topicPartitions
      .map(tp => {
        tp -> Option(table.properties().get(offsetKey(tp.topic(), tp.partition()))).map(offsetWatermark => {
          val Array(o, w) = offsetWatermark.split(':')
          StreamPosition(o.toLong + 1, Timestamp(w.toLong))
        })
      })
      .toMap
  }
}
