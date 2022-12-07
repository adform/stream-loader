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
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.Optional
import scala.collection.mutable
import scala.concurrent.duration._

class RecordBatchingSinkerTest extends AnyFunSpec with Matchers {

  case class TestBatch(recordRanges: Seq[RecordRange], var isDiscarded: Boolean) extends RecordBatch {
    override def discard(): Boolean = {
      isDiscarded = true
      true
    }
  }

  class TestBatchProvider {
    val batches: mutable.ListBuffer[TestBatch] = mutable.ListBuffer.empty

    def newBatch(recordRanges: Seq[RecordRange]): TestBatch = {
      val batch = TestBatch(recordRanges, isDiscarded = false)
      batches.addOne(batch)
      batch
    }
  }

  val tp = new TopicPartition("test-topic", 0)
  val recordsPerBatch = 10
  val batchCount = 10

  def createRecords(
      topic: String,
      partition: Int,
      initialTimestamp: Long,
      initialOffset: Int,
      recordCount: Int): Seq[Record] =
    for (offset <- initialOffset until (initialOffset + recordCount))
      yield {
        val timestamp = initialTimestamp + offset * 1000
        Record(
          new ConsumerRecord[Array[Byte], Array[Byte]](
            topic,
            partition,
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            ConsumerRecord.NULL_SIZE,
            ConsumerRecord.NULL_SIZE,
            Array.emptyByteArray,
            Array.emptyByteArray,
            new RecordHeaders,
            Optional.empty[Integer]
          ),
          Timestamp(timestamp)
        )
      }

  def createTestRecords(initialTimestamp: Long): Seq[Record] =
    createRecords(tp.topic(), tp.partition(), initialTimestamp, initialOffset = 0, recordsPerBatch * batchCount)

  def newTestSinker(batchProvider: TestBatchProvider = new TestBatchProvider): RecordBatchingSinker[TestBatch] = {
    val sinker: RecordBatchingSinker[TestBatch] =
      new RecordBatchingSinker[TestBatch](
        groupName = "root",
        groupPartitions = Set(tp),
        () =>
          new RecordBatchBuilder[TestBatch] {
            override def isBatchReady: Boolean = currentRecordCount >= recordsPerBatch
            override def build(): Option[TestBatch] = Some(batchProvider.newBatch(currentRecordRanges))
            override def discard(): Unit = {}
        },
        new RecordBatchStorage[TestBatch] {
          override def recover(topicPartitions: Set[TopicPartition]): Unit = {}
          override def committedPositions(
              topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = Map(tp -> None)
          override def commitBatch(batch: TestBatch): Unit = {}
        },
        batchCommitQueueSize = 1,
        retryPolicy = Retry.Policy(0, 0.seconds, 0)
      )

    sinker.initialize(new MockKafkaContext)
    sinker
  }

  it("should discard all batches after committing them") {
    val batchProvider = new TestBatchProvider
    val sinker = newTestSinker(batchProvider)

    createTestRecords(System.currentTimeMillis()).foreach(sinker.write)

    while (sinker.commitQueueSize > 0) Thread.sleep(10) // wait until the commit queue clears

    batchProvider.batches.forall(_.isDiscarded) shouldBe true
  }
}
