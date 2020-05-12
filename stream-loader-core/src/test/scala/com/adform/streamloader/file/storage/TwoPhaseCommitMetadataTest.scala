/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TwoPhaseCommitMetadataTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  implicit val arbitraryTimestamp: Arbitrary[Timestamp] = Arbitrary(Arbitrary.arbLong.arbitrary.map(Timestamp))
  implicit val arbitraryPosition: Arbitrary[StreamPosition] = Arbitrary(Gen.resultOf(StreamPosition.apply _))
  implicit val arbitraryStaging: Arbitrary[FileStaging] = Arbitrary(Gen.resultOf(FileStaging))
  implicit val arbitraryStaged: Arbitrary[StagedOffsetCommit] = Arbitrary(Gen.resultOf(StagedOffsetCommit))
  implicit val arbitraryMetadata: Arbitrary[TwoPhaseCommitMetadata] = Arbitrary(
    Gen.resultOf(TwoPhaseCommitMetadata.apply _))

  it("should deserialize commit metadata with staged data") {
    val json = """{
                 |  "watermark": 1570097839000,
                 |  "staged": {
                 |    "staged_file_path": "dt=20190901/file.parquet.part",
                 |    "destination_file_path": "dt=20190901/file.parquet",
                 |    "start_offset": 123,
                 |    "start_watermark": 1570097639000,
                 |    "end_offset": 256,
                 |    "end_watermark": 1570097839000
                 |  }
                 |}""".stripMargin
    val parsedMetadata = TwoPhaseCommitMetadata.tryParseJson(json)

    val expectedMetadata =
      TwoPhaseCommitMetadata(
        Timestamp(1570097839000L),
        Some(
          StagedOffsetCommit(
            FileStaging("dt=20190901/file.parquet.part", "dt=20190901/file.parquet"),
            StreamPosition(123, Timestamp(1570097639000L)),
            StreamPosition(256, Timestamp(1570097839000L))
          )
        )
      )

    parsedMetadata shouldEqual Some(expectedMetadata)
  }

  it("should deserialize commit metadata without staged data") {
    val json = """{ "watermark": 1570097839000 }"""
    val parsedMetadata = TwoPhaseCommitMetadata.tryParseJson(json)

    val expectedMetadata = TwoPhaseCommitMetadata(Timestamp(1570097839000L), None)

    parsedMetadata shouldEqual Some(expectedMetadata)
  }

  it("should be idempotent when serializing and de-serializing") {
    forAll { (metadata: TwoPhaseCommitMetadata) =>
      TwoPhaseCommitMetadata.tryParseJson(metadata.toJson) shouldEqual Some(metadata)
    }
  }
}
