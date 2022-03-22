/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch.storage

import java.io.File

import com.adform.streamloader.MockKafkaContext
import com.adform.streamloader.file.SingleFileRecordBatch
import com.adform.streamloader.model.{RecordRange, StreamPosition, Timestamp}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer

class InDataOffsetFileStorageTest extends AnyFunSpec with Matchers {

  private val exampleFile = SingleFileRecordBatch(
    new File("/tmp/file.parquet"),
    Seq(
      RecordRange(
        "topic",
        0,
        StreamPosition(0, Timestamp(1570109555000L)),
        StreamPosition(100, Timestamp(1570109655000L))),
      RecordRange(
        "topic",
        1,
        StreamPosition(50, Timestamp(1570109565000L)),
        StreamPosition(150, Timestamp(1570109685000L)))
    )
  )

  private val topicPartitions = exampleFile.recordRanges.map(r => new TopicPartition(r.topic, r.partition)).toSet

  class MockInDataOffsetFileStorage extends InDataOffsetBatchStorage[SingleFileRecordBatch] {
    val storedFiles: ListBuffer[File] = ListBuffer.empty[File]
    override def commitBatchWithOffsets(batch: SingleFileRecordBatch): Unit = storedFiles.addOne(batch.file)
    override def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] =
      Map.empty
  }

  it("should store the file and commit offsets to Kafka with the watermark in the commit metadata") {
    val context = new MockKafkaContext()
    val storage = new MockInDataOffsetFileStorage()

    storage.initialize(context)
    storage.commitBatch(exampleFile)

    val expectedOffsets = exampleFile.recordRanges.map(
      r =>
        new TopicPartition(r.topic, r.partition) -> Some(
          new OffsetAndMetadata(r.end.offset + 1, s"""{ "watermark": ${r.end.watermark.millis} }""")))

    storage.storedFiles should contain theSameElementsAs Seq(exampleFile.file)
    context.committed(topicPartitions) should contain theSameElementsAs expectedOffsets
  }
}
