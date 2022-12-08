/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import com.adform.streamloader.model.{StreamRange, StreamPosition, Timestamp}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.LocalDate
import java.util.UUID

class TimePartitioningFilePathFormatterTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  val recordRangeGen: Gen[StreamRange] = for {
    topicLength <- Gen.choose(1, 20)
    topic <- Gen.listOfN(topicLength, Gen.oneOf(Gen.alphaNumChar, Gen.oneOf('-', '_', '.')))
    partition <- Gen.chooseNum(0, 256)
    startOffset <- Gen.choose(0, Long.MaxValue - Int.MaxValue - 1)
    endOffset <- Gen.posNum[Int].map(diff => startOffset + diff)
    startWatermark <- Gen.choose(1262304000000L, 1893456000000L) // 2010-01-01 - 2020-01-01
    endWatermark <- Gen.choose(0, 631152000000L).map(diff => startWatermark + diff) // 10 years
  } yield
    StreamRange(
      new String(topic.toArray),
      partition,
      StreamPosition(startOffset, Timestamp(startWatermark)),
      StreamPosition(endOffset, Timestamp(endWatermark))
    )

  val recordRangeSeqGen: Gen[Seq[StreamRange]] = Gen.choose(1, 5).flatMap(i => Gen.listOfN(i, recordRangeGen))

  implicit val arbitraryRecordRange: Arbitrary[StreamRange] = Arbitrary(recordRangeGen)
  implicit val arbitraryRecordRangeSeq: Arbitrary[Seq[StreamRange]] = Arbitrary(recordRangeSeqGen)

  implicit val arbitraryLocalDate: Arbitrary[LocalDate] = {
    Arbitrary {
      for {
        epochDay <- Gen.chooseNum(LocalDate.of(2000, 1, 1).toEpochDay, LocalDate.of(3000, 1, 1).toEpochDay)
      } yield LocalDate.ofEpochDay(epochDay)
    }
  }

  it("should format filenames for files with a single record range") {
    val formatter =
      new TimePartitioningFilePathFormatter[LocalDate](Some("'dt='yyyyMMdd"), Compression.LZOP.fileExtension)
    val ranges =
      StreamRange(
        "test-topic",
        0,
        StreamPosition(100, Timestamp(1554897174000L)),
        StreamPosition(200, Timestamp(1554897175000L)))

    val formatted = formatter.formatPath(LocalDate.parse("2019-04-10"), Seq(ranges))

    formatted should startWith("dt=20190410")
    formatted should endWith(".lzo")

    noException should be thrownBy UUID.fromString(formatted.substring(12, formatted.length - 4))
  }

  it("should format filenames for files with multiple record ranges") {
    val formatter =
      new TimePartitioningFilePathFormatter[LocalDate](Some("'dt='yyyyMMdd"), Compression.LZOP.fileExtension)
    val ranges = Seq(
      StreamRange(
        "test-topic",
        0,
        StreamPosition(100, Timestamp(1554897174000L)),
        StreamPosition(200, Timestamp(1554897175000L))),
      StreamRange(
        "test-topic",
        1,
        StreamPosition(300, Timestamp(1554897174100L)),
        StreamPosition(400, Timestamp(1554897175200L)))
    )

    val formatted = formatter.formatPath(LocalDate.parse("2019-04-10"), ranges)

    formatted should startWith("dt=20190410")
    formatted should endWith(".lzo")

    noException should be thrownBy UUID.fromString(formatted.substring(12, formatted.length - 4))
  }

  it("should format filenames correctly for arbitrary ranges") {
    val formatter =
      new TimePartitioningFilePathFormatter[LocalDate](Some("'dt='yyyyMMdd"), Compression.LZOP.fileExtension)
    forAll { batches: Seq[StreamRange] =>
      val formatted = formatter.formatPath(LocalDate.parse("2019-04-10"), batches)

      formatted should startWith("dt=")
      formatted should endWith(".lzo")

      noException should be thrownBy formatted.substring(3, 11).toLong
      noException should be thrownBy UUID.fromString(formatted.substring(12, formatted.length - 4))
    }
  }

  it("should format filenames reproducibly") {
    val formatter = new TimePartitioningFilePathFormatter[LocalDate](None, None)
    forAll { (date: LocalDate, batches: Seq[StreamRange]) =>
      formatter.formatPath(date, batches) shouldEqual formatter.formatPath(date, batches)
    }
  }
}
