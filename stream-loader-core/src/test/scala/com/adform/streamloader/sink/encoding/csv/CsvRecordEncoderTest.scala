/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.encoding.csv

import java.util.UUID
import com.adform.streamloader.model.Timestamp
import com.adform.streamloader.sink.encoding.csv.{CsvRecordEncoder, CsvTypeEncoder}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CsvRecordEncoderTest extends AnyFunSpec with Matchers {

  case class BinaryInt(value: Int)

  class BinaryIntEncoder extends CsvTypeEncoder[BinaryInt] {
    override def encode(t: BinaryInt): String = t.value.toBinaryString
  }

  implicit val binaryIntEncoder: BinaryIntEncoder = new BinaryIntEncoder

  case class BasicRecord(a: Byte, b: Short, c: Int, d: Long, e: Char, f: String, g: Boolean)
  case class ComplexRecord(uuid: UUID, float: Float, double: Double, time: Timestamp)
  case class OptionalRecord(a: Option[Int], b: Option[String])
  case class CustomRecord(i: BinaryInt)

  def encoderFor[T: CsvRecordEncoder]: CsvRecordEncoder[T] = implicitly[CsvRecordEncoder[T]]

  it("should derive field names") {
    encoderFor[BasicRecord].columnNames shouldEqual Array("a", "b", "c", "d", "e", "f", "g")
  }

  it("should encode basic data types") {
    encoderFor[BasicRecord].encode(BasicRecord(1, 2, 3, 4, 'x', "abcd", g = true)) shouldEqual
      Array(Some("1"), Some("2"), Some("3"), Some("4"), Some("x"), Some("abcd"), Some("true"))
  }

  it("should encode complex data types") {
    encoderFor[ComplexRecord].encode(
      ComplexRecord(UUID.fromString("28065835-c674-4476-b534-4d53ed2906d8"), 1.2f, 3.3, Timestamp(1554296316129L))
    ) shouldEqual Array(
      Some("28065835-c674-4476-b534-4d53ed2906d8"),
      Some("1.2"),
      Some("3.3"),
      Some("2019-04-03 12:58:36.129")
    )
  }

  it("should encode optional data types") {
    encoderFor[OptionalRecord].encode(OptionalRecord(None, None)) shouldEqual Array(None, None)
    encoderFor[OptionalRecord].encode(OptionalRecord(Some(1), Some("test"))) shouldEqual Array(Some("1"), Some("test"))
  }

  it("should encode custom data types") {
    encoderFor[CustomRecord].encode(CustomRecord(BinaryInt(23))) shouldEqual Array(Some("10111"))
  }
}
