/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink

import com.adform.streamloader.model.Generators._
import com.adform.streamloader.model.StreamInterval._
import com.adform.streamloader.model.{StreamInterval, StreamPosition, StreamRecord, Timestamp}
import com.adform.streamloader.source.{KafkaContext, MockKafkaContext}
import com.adform.streamloader.util.TestExtensions._
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Gen
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Duration
import scala.collection.mutable

class RewindingPartitionGroupSinkerTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class TestBaseSinker(committedPositions: Map[TopicPartition, Option[StreamPosition]]) extends PartitionGroupSinker {

    val writtenRecords: mutable.ArrayBuffer[StreamRecord] = mutable.ArrayBuffer.empty

    override val groupName: String = "test-group"
    override val groupPartitions: Set[TopicPartition] = committedPositions.keySet

    override def initialize(kc: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
      writtenRecords.clear()
      committedPositions
    }

    override def write(record: StreamRecord): Unit = writtenRecords.addOne(record)

    override def heartbeat(): Unit = {}

    override def close(): Unit = {}
  }

  class TestRewindingSinker(baseSinker: TestBaseSinker, interval: StreamInterval)
      extends RewindingPartitionGroupSinker(baseSinker, interval) {

    val touchedRecords: mutable.ArrayBuffer[StreamRecord] = mutable.ArrayBuffer.empty

    override def initialize(kafkaContext: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
      val positions = super.initialize(kafkaContext)
      touchedRecords.clear()
      positions
    }

    override def touchRewoundRecord(record: StreamRecord): Unit = touchedRecords.addOne(record)
  }

  describe("rewinding sinker") {

    val kafka = new MockKafkaContext()

    val initRecords = Seq(
      newStreamRecord("test", 0, 0L, Timestamp(0L)),
      newStreamRecord("test", 0, 10L, Timestamp(100L)),
      newStreamRecord("test", 0, 20L, Timestamp(90L)),
      newStreamRecord("test", 0, 30L, Timestamp(200L)),
      newStreamRecord("test", 1, 0L, Timestamp(0L)),
      newStreamRecord("test", 1, 10L, Timestamp(100L)),
      newStreamRecord("test", 1, 20L, Timestamp(300L))
    )

    initRecords.foreach(r => kafka.write(r))
    val committedPositions = kafka.commitWrites() + (new TopicPartition("test", 2) -> None)

    val baseSinker = new TestBaseSinker(committedPositions)

    describe("rewind offset calculation with numeric ranges") {

      it("should be a no-op with a zero offset") {
        val rewindingSinker = new TestRewindingSinker(baseSinker, OffsetRange(0L))

        val rewound = rewindingSinker.initialize(kafka)

        rewound should contain theSameElementsAs Map(
          new TopicPartition("test", 0) -> Some(StreamPosition(31L, Timestamp(200L))),
          new TopicPartition("test", 1) -> Some(StreamPosition(21L, Timestamp(300L))),
          new TopicPartition("test", 2) -> None
        )
      }

      it("should be correct with a valid offset") {
        val rewindingSinker = new TestRewindingSinker(baseSinker, OffsetRange(10L))

        val rewound = rewindingSinker.initialize(kafka)

        rewound should contain theSameElementsAs Map(
          new TopicPartition("test", 0) -> Some(StreamPosition(21L, Timestamp(200L))),
          new TopicPartition("test", 1) -> Some(StreamPosition(11L, Timestamp(300L))),
          new TopicPartition("test", 2) -> None
        )
      }

      it("should be capped given an offset that is out of range") {
        val rewindingSinker = new TestRewindingSinker(baseSinker, OffsetRange(1000L))

        val rewound = rewindingSinker.initialize(kafka)

        rewound should contain theSameElementsAs Map(
          new TopicPartition("test", 0) -> Some(StreamPosition(0L, Timestamp(200L))),
          new TopicPartition("test", 1) -> Some(StreamPosition(0L, Timestamp(300L))),
          new TopicPartition("test", 2) -> None
        )
      }
    }

    describe("rewind offset calculation with watermark ranges") {
      it("should be a no-op with a zero offset") {
        val rewindingSinker = new TestRewindingSinker(baseSinker, WatermarkRange(Duration.ofMillis(0)))

        val rewound = rewindingSinker.initialize(kafka)

        rewound should contain theSameElementsAs Map(
          new TopicPartition("test", 0) -> Some(StreamPosition(31L, Timestamp(200L))),
          new TopicPartition("test", 1) -> Some(StreamPosition(21L, Timestamp(300L))),
          new TopicPartition("test", 2) -> None
        )
      }

      it("should be correct with a valid offset") {
        val rewindingSinker = new TestRewindingSinker(baseSinker, WatermarkRange(Duration.ofMillis(150L)))

        val rewound = rewindingSinker.initialize(kafka)

        rewound should contain theSameElementsAs Map(
          new TopicPartition("test", 0) -> Some(StreamPosition(10L, Timestamp(100L))),
          new TopicPartition("test", 1) -> Some(StreamPosition(20L, Timestamp(300L))),
          new TopicPartition("test", 2) -> None
        )
      }

      it("should be capped given an offset that is out of range") {
        val rewindingSinker = new TestRewindingSinker(baseSinker, WatermarkRange(Duration.ofHours(10)))

        val rewound = rewindingSinker.initialize(kafka)

        rewound should contain theSameElementsAs Map(
          new TopicPartition("test", 0) -> Some(StreamPosition(0L, Timestamp(0L))),
          new TopicPartition("test", 1) -> Some(StreamPosition(0L, Timestamp(0L))),
          new TopicPartition("test", 2) -> None
        )
      }
    }

    describe(s"message writing") {
      val rewindingSinker = new TestRewindingSinker(baseSinker, OffsetRange(100L))
      rewindingSinker.initialize(kafka)

      it("should touch and not write warm-up records") {
        initRecords.foreach(r => rewindingSinker.write(r))

        baseSinker.writtenRecords shouldBe empty
        rewindingSinker.touchedRecords.map(_.tpo) should contain theSameElementsInOrderAs initRecords.map(_.tpo)
      }

      it("should pass through all new records afterwards") {
        val records = Seq(
          newStreamRecord("test", 0, 31L, Timestamp(220L)),
          newStreamRecord("test", 0, 32L, Timestamp(219L)),
          newStreamRecord("test", 1, 21L, Timestamp(299L)),
          newStreamRecord("test", 1, 22L, Timestamp(310L)),
          newStreamRecord("test", 2, 0L, Timestamp(300L))
        )
        records.foreach(r => rewindingSinker.write(r))

        baseSinker.writtenRecords.map(_.tpo) should contain theSameElementsInOrderAs records.map(_.tpo)
      }
    }

    it("should only pass through new messages for any rewind range") {

      val rewindRanges = for {
        range <- Gen.choose(0L, 1000L)
        rangeType <- Gen.oneOf(
          (r: Long) => OffsetRange(r),
          (r: Long) => WatermarkRange(Duration.ofMillis(r))
        )
      } yield rangeType(range)

      forAll(rewindRanges) { rewindRange =>
        {
          val rewindingSinker = new TestRewindingSinker(baseSinker, rewindRange)
          rewindingSinker.initialize(kafka)

          val records = Seq(
            newStreamRecord("test", 0, 31L, Timestamp(220L)),
            newStreamRecord("test", 0, 32L, Timestamp(219L)),
            newStreamRecord("test", 1, 21L, Timestamp(299L)),
            newStreamRecord("test", 1, 22L, Timestamp(310L)),
            newStreamRecord("test", 2, 0L, Timestamp(300L))
          )
          records.foreach(r => rewindingSinker.write(r))

          baseSinker.writtenRecords.map(_.tpo) should contain theSameElementsInOrderAs records.map(_.tpo)
        }
      }
    }
  }
}
