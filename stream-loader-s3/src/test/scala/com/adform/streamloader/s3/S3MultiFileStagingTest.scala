/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.s3

import com.adform.streamloader.util.JsonSerializer
import org.json4s.JsonAST.JValue
import org.json4s.{JArray, JObject, JString}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class S3MultiFileStagingTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  private val serializer = implicitly[JsonSerializer[S3MultiFileStaging]]

  val testStaging: S3MultiFileStaging = S3MultiFileStaging(
    Seq(S3FileStaging("upload1", "part1", "key1"), S3FileStaging("upload2", "part2", "key2"))
  )

  val testStagingJson: JValue = JArray(
    List(
      JObject("id" -> JString("upload1"), "part_tag" -> JString("part1"), "key" -> JString("key1")),
      JObject("id" -> JString("upload2"), "part_tag" -> JString("part2"), "key" -> JString("key2"))
    )
  )

  val singleStagingGen: Gen[S3FileStaging] = for {
    uploadId <- Gen.asciiPrintableStr
    tag <- Gen.asciiPrintableStr
    key <- Gen.asciiPrintableStr
  } yield S3FileStaging(uploadId, tag, key)

  val stagingGen: Gen[S3MultiFileStaging] = Gen.listOf(singleStagingGen).map(l => S3MultiFileStaging(l))

  implicit val arbitraryStaging: Arbitrary[S3MultiFileStaging] = Arbitrary(stagingGen)

  it("should serialize correctly") {
    serializer.serialize(testStaging) shouldEqual testStagingJson
  }

  it("should deserialize correctly") {
    serializer.deserialize(testStagingJson) shouldEqual testStaging
  }

  it("should serialize and deserialize to the same thing") {
    forAll { (staging: S3MultiFileStaging) =>
      {
        serializer.deserialize(serializer.serialize(staging)) shouldEqual staging
      }
    }
  }
}
