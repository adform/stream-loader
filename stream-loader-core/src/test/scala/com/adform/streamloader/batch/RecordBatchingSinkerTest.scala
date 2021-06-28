/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch

import com.adform.streamloader.MockKafkaContext
import com.adform.streamloader.batch.storage.RecordBatchStorage
import com.adform.streamloader.model._
import com.adform.streamloader.util.Retry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class RecordBatchingSinkerTest extends AnyFunSpec with Matchers {

  type Record = ConsumerRecord[Array[Byte], Array[Byte]]

  case class TestBatch(recordRanges: Seq[RecordRange]) extends RecordBatch

  val tp = new TopicPartition("test-topic", 0)
  val recordsPerBatch = 10
  val batchCount = 10

  val validWatermarkDiffMillis: Long = 1 * 60 * 60 * 1000

  val validTimestampMillis: Long = System.currentTimeMillis()
  val nonValidTimestampMillis: Long = System.currentTimeMillis() + validWatermarkDiffMillis * 2

  def createRecords(
      topic: String,
      partition: Int,
      initialTimestamp: Long,
      initialOffset: Int,
      recordCount: Int
  ): Seq[Record] =
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

  def createTestRecords(initialTimestamp: Long): Seq[Record] =
    createRecords(tp.topic(), tp.partition(), initialTimestamp, initialOffset = 0, recordsPerBatch * batchCount)

  def newAssertiveSinker(shouldUpdateWatermark: Boolean): RecordBatchingSinker[TestBatch] = {
    var maxTimestamp = Timestamp(-1L)

    val sinker: RecordBatchingSinker[TestBatch] =
      new RecordBatchingSinker[TestBatch](
        groupName = "root",
        groupPartitions = Set(tp),
        () =>
          new RecordBatchBuilder[TestBatch] {
            override def isBatchReady: Boolean = currentRecordCount >= recordsPerBatch
            override def build(): Option[TestBatch] = Some(TestBatch(currentRecordRanges))
            override def discard(): Unit = {}
        },
        new RecordBatchStorage[TestBatch] {
          override def recover(topicPartitions: Set[TopicPartition]): Unit = {}
          override def committedPositions(
              topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = Map(tp -> None)
          override def commitBatch(batch: TestBatch): Unit = {
            val watermark = batch.recordRanges.head.end.watermark
            if (shouldUpdateWatermark) {
              assert(batch.recordRanges.head.end.watermark.millis > maxTimestamp.millis)
            } else {
              assert(batch.recordRanges.head.end.watermark == maxTimestamp)
            }
            maxTimestamp = watermark
          }
        },
        batchCommitQueueSize = 1,
        validWatermarkDiffMillis = validWatermarkDiffMillis,
        retryPolicy = Retry.Policy(0, 0.seconds, 0)
      )

    sinker.initialize(new MockKafkaContext)
    sinker
  }

  it("should push the watermark forward if record timestamp is greater and valid") {
    val sinker = newAssertiveSinker(shouldUpdateWatermark = true)
    createTestRecords(validTimestampMillis).foreach(sinker.write)
  }

  it("should not update watermark if record timestamp is greater but exceeds permitted time window") {
    val sinker = newAssertiveSinker(shouldUpdateWatermark = false)
    createTestRecords(nonValidTimestampMillis).foreach(sinker.write)
  }
}
