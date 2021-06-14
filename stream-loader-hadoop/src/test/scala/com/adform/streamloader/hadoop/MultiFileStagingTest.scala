/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop

import com.adform.streamloader.util.JsonSerializer
import org.json4s.JsonAST.JValue
import org.json4s.{JArray, JObject, JString}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MultiFileStagingTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  private val serializer = implicitly[JsonSerializer[MultiFileStaging]]

  val testStaging: MultiFileStaging = MultiFileStaging(
    Seq(
      FileStaging("staged1", "destination1"),
      FileStaging("staged2", "destination2")
    ))

  val testStagingJson: JValue = JArray(
    List(
      JObject("staged_file_path" -> JString("staged1"), "destination_file_path" -> JString("destination1")),
      JObject("staged_file_path" -> JString("staged2"), "destination_file_path" -> JString("destination2"))
    ))

  val singleStagingGen: Gen[FileStaging] = for {
    staged <- Gen.asciiPrintableStr
    destination <- Gen.asciiPrintableStr
  } yield FileStaging(staged, destination)

  val stagingGen: Gen[MultiFileStaging] = Gen.listOf(singleStagingGen).map(l => MultiFileStaging(l))

  implicit val arbitraryStaging: Arbitrary[MultiFileStaging] = Arbitrary(stagingGen)

  it("should serialize correctly") {
    serializer.serialize(testStaging) shouldEqual testStagingJson
  }

  it("should deserialize correctly") {
    serializer.deserialize(testStagingJson) shouldEqual testStaging
  }

  it("should serialize and deserialize to the same thing") {
    forAll { (staging: MultiFileStaging) =>
      {
        serializer.deserialize(serializer.serialize(staging)) shouldEqual staging
      }
    }
  }
}
