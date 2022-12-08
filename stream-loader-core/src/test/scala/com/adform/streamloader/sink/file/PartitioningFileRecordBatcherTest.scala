/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

import java.io.File
import java.nio.file.Files
import java.util.Optional
import com.adform.streamloader.model.{StreamPosition, StreamRange, StreamRecord, Timestamp}
import com.adform.streamloader.sink.encoding.csv.CsvFileBuilder
import com.adform.streamloader.sink.file.{Compression, PartitioningFileRecordBatcher}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.util.Using

class PartitioningFileRecordBatcherTest extends AnyFunSpec with Matchers {

  describe("mod 10 partitioning batcher with a 100 records") {

    val batcher = new PartitioningFileRecordBatcher[Int, String](
      (record: StreamRecord) => Seq(new String(record.consumerRecord.value(), "UTF-8")),
      (record, value) => value.toInt % 10,
      _ => new CsvFileBuilder[String](Compression.NONE),
      stats => stats.exists(f => f.recordsWritten >= 20)
    )

    describe("with no records") {
      val builder = batcher.newBatchBuilder()
      val batch = builder.build()

      it("should produce an empty batch") {
        batch shouldEqual None
      }
    }

    describe("with a 100 records in all partitions") {

      val builder = batcher.newBatchBuilder()
      for (i <- 0 until 100) {
        builder.add(newRecord("topic", 0, Timestamp(i), i, "key", i.toString))
      }

      it("should not be ready yet") {
        builder.isBatchReady shouldEqual false
      }

      val maybePartitionedBatch = builder.build()

      it("should produce a bath") {
        maybePartitionedBatch.nonEmpty shouldBe true
      }

      val partitionedBatch = maybePartitionedBatch.get

      it("should produce 10 partitions") {
        partitionedBatch.partitionBatches.size shouldEqual 10
      }

      it("should produce batches with 10 records each") {
        partitionedBatch.partitionBatches.values.forall(b => readAllLines(b.file).size == 10) shouldEqual true
      }

      it("should produce correctly partitioned batches") {
        partitionedBatch.partitionBatches.foreach { case (partition, batch) =>
          readAllLines(batch.file).forall(line => line.toInt % 10 == partition) shouldBe true
        }
      }

      it("should have correct overall record ranges") {
        partitionedBatch.recordRanges should contain theSameElementsAs
          Seq(StreamRange("topic", 0, StreamPosition(0, Timestamp(0)), StreamPosition(99, Timestamp(99))))
      }
    }

    describe("with 30 records in the same partition") {

      val builder = batcher.newBatchBuilder()
      for (i <- 0 until 1000) {
        builder.add(newRecord("topic", 0, Timestamp(i), i, "key", "1"))
      }

      it("should be ready by now") {
        builder.isBatchReady shouldBe true
      }

      val partitionedBatch = builder.build()

      it("should contain a single partition batch") {
        partitionedBatch.get.partitionBatches.size shouldEqual 1
      }
    }

    describe("cleanup") {

      val builder = batcher.newBatchBuilder()
      for (i <- 0 until 100) {
        builder.add(newRecord("topic", 0, Timestamp(i), i, "key", "1"))
      }
      val batch = builder.build().get
      val files = batch.fileBatches.map(_.file)

      batch.discard()

      it("should delete all files after discarding batch") {
        files.exists(_.exists()) shouldBe false
      }
    }
  }

  def newRecord(
      topic: String,
      partition: Int,
      timestamp: Timestamp,
      offset: Long,
      key: String,
      value: String
  ): StreamRecord = {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      partition,
      offset,
      timestamp.millis,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      key.getBytes("UTF-8"),
      value.getBytes("UTF-8"),
      new RecordHeaders,
      Optional.empty[Integer]
    )
    StreamRecord(cr, timestamp)
  }

  def readAllLines(file: File): Seq[String] = Using.resource(Files.lines(file.toPath))(_.iterator().asScala.toSeq)
}
