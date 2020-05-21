/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import com.adform.streamloader.MockKafkaContext
import com.adform.streamloader.encoding.csv.CsvFileBuilderFactory
import com.adform.streamloader.file.storage.FileStorage
import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.Retry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.tailrec
import scala.concurrent.duration._

class FilePartitionGroupSinkerTest extends AnyFunSpec with Matchers {

  type Record = ConsumerRecord[Array[Byte], Array[Byte]]
  type Batch = Seq[Record]

  val tp = new TopicPartition("test-topic", 0)
  val recordsPerBatch = 10
  val batchCount = 10

  val validWatermarkDiffMillis: Long = 1 * 60 * 60 * 1000

  val validTimestampMillis: Long = System.currentTimeMillis()
  val nonValidTimestampMillis: Long = System.currentTimeMillis() + validWatermarkDiffMillis * 2

  def createBatch(
      topic: String,
      partition: Int,
      initialTimestamp: Long,
      initialOffset: Int,
      recordCount: Int
  ): Batch =
    for (offset <- initialOffset until (initialOffset + recordCount))
      yield
        new Record(
          topic,
          partition,
          offset,
          initialTimestamp + offset * 1000,
          TimestampType.CREATE_TIME,
          ConsumerRecord.NULL_CHECKSUM,
          ConsumerRecord.NULL_SIZE,
          ConsumerRecord.NULL_SIZE,
          Array.emptyByteArray,
          Array.emptyByteArray
        )

  def createBatches(
      topic: String,
      partition: Int,
      initialTimestamp: Long,
      recordsPerBatch: Int,
      batchCount: Int): Seq[Batch] = {
    @tailrec def loop(batches: List[Batch], initialOffset: Int, remainingBatchCount: Int): Seq[Batch] =
      if (remainingBatchCount > 0) {
        val batch = createBatch(topic, partition, initialTimestamp, initialOffset, recordsPerBatch)
        loop(batch :: batches, initialOffset + recordsPerBatch, remainingBatchCount - 1)
      } else batches
    loop(List.empty, 0, batchCount).reverse
  }

  def createTestBatches(initialTimestamp: Long): Seq[Batch] =
    createBatches(tp.topic(), tp.partition(), initialTimestamp, recordsPerBatch, batchCount)

  def newAssertiveLoader(shouldUpdateWatermark: Boolean): FilePartitionGroupSinker[String, Unit] = {
    val commitStrategy = FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(recordsPerBatch))
    val fileBuilderFactory = new CsvFileBuilderFactory[String](Compression.NONE)

    var maxTimestamp = Timestamp(-1L)

    val loader: FilePartitionGroupSinker[String, Unit] =
      new FilePartitionGroupSinker[String, Unit](
        groupName = "root",
        Set(tp),
        (_, _) => Seq(""),
        fileBuilderFactory,
        new FileStorage[Unit] {
          override def startNewFile(): Unit = {}
          override def recover(topicPartitions: Set[TopicPartition]): Unit = {}
          override def committedPositions(
              topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = Map(tp -> None)
          override def commitFile(file: RecordRangeFile[Unit]): Unit = {
            val watermark = file.recordRanges.head.end.watermark
            if (shouldUpdateWatermark) {
              assert(file.recordRanges.head.end.watermark.millis > maxTimestamp.millis)
            } else {
              assert(file.recordRanges.head.end.watermark == maxTimestamp)
            }
            maxTimestamp = watermark
          }
        },
        commitStrategy,
        fileCommitQueueSize = 1,
        validWatermarkDiffMillis = validWatermarkDiffMillis,
        retryPolicy = Retry.Policy(0, 0.seconds, 0)
      )

    loader.initialize(new MockKafkaContext)
    loader
  }

  it("should push the watermark forward if record timestamp is greater and valid") {
    val loader = newAssertiveLoader(shouldUpdateWatermark = true)

    createTestBatches(validTimestampMillis).foreach { batch =>
      batch.foreach(loader.write)
    }
  }

  it("should not update watermark if record timestamp is greater but exceeds permitted time window") {
    val loader = newAssertiveLoader(shouldUpdateWatermark = false)

    createTestBatches(nonValidTimestampMillis).foreach { batch =>
      batch.foreach(loader.write)
    }
  }
}
