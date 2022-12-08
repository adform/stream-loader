/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch.storage

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.sink.batch.storage.{StagedOffsetCommit, TwoPhaseCommitMetadata}
import com.adform.streamloader.util.JsonSerializer
import org.json4s.JString
import org.json4s.JsonAST.JValue
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

case class TestStaging(str: String)

object TestStaging {

  implicit val jsonSerializer: JsonSerializer[TestStaging] = new JsonSerializer[TestStaging] {
    override def serialize(value: TestStaging): JValue = JString(value.str)

    override def deserialize(json: JValue): TestStaging = json match {
      case JString(str) => TestStaging(str)
      case _ => throw new IllegalArgumentException("Invalid staging JSON")
    }
  }
}

class TwoPhaseCommitMetadataTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  implicit val arbitraryTimestamp: Arbitrary[Timestamp] = Arbitrary(Arbitrary.arbLong.arbitrary.map(Timestamp))
  implicit val arbitraryPosition: Arbitrary[StreamPosition] = Arbitrary(Gen.resultOf(StreamPosition.apply _))
  implicit val arbitraryStaging: Arbitrary[TestStaging] = Arbitrary(Gen.resultOf(TestStaging.apply _))
  implicit val arbitraryStaged: Arbitrary[StagedOffsetCommit[TestStaging]] = Arbitrary(
    Gen.resultOf(StagedOffsetCommit.apply[TestStaging] _)
  )
  implicit val arbitraryMetadata: Arbitrary[TwoPhaseCommitMetadata[TestStaging]] = Arbitrary(
    Gen.resultOf(TwoPhaseCommitMetadata.apply[TestStaging] _)
  )

  it("should deserialize commit metadata with staged data") {
    val json = """{
                 |  "watermark": 1570097839000,
                 |  "staged": {
                 |    "staging": "test",
                 |    "start_offset": 123,
                 |    "start_watermark": 1570097639000,
                 |    "end_offset": 256,
                 |    "end_watermark": 1570097839000
                 |  }
                 |}""".stripMargin
    val parsedMetadata = TwoPhaseCommitMetadata.tryParseJson[TestStaging](json)

    val expectedMetadata =
      TwoPhaseCommitMetadata(
        Timestamp(1570097839000L),
        Some(
          StagedOffsetCommit(
            TestStaging("test"),
            StreamPosition(123, Timestamp(1570097639000L)),
            StreamPosition(256, Timestamp(1570097839000L))
          )
        )
      )

    parsedMetadata shouldEqual Some(expectedMetadata)
  }

  it("should deserialize commit metadata without staged data") {
    val json = """{ "watermark": 1570097839000 }"""
    val parsedMetadata = TwoPhaseCommitMetadata.tryParseJson[TestStaging](json)

    val expectedMetadata = TwoPhaseCommitMetadata[TestStaging](Timestamp(1570097839000L), None)

    parsedMetadata shouldEqual Some(expectedMetadata)
  }

  it("should be idempotent when serializing and de-serializing") {
    forAll { (metadata: TwoPhaseCommitMetadata[TestStaging]) =>
      TwoPhaseCommitMetadata.tryParseJson[TestStaging](metadata.toJson) shouldEqual Some(metadata)
    }
  }
}
