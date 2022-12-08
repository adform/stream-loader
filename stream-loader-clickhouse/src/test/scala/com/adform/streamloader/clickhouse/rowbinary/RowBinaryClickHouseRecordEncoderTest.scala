/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import java.io.ByteArrayOutputStream

import com.adform.streamloader.encoding.macros.DataTypeEncodingAnnotation._
import com.adform.streamloader.model.Timestamp
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RowBinaryClickHouseRecordEncoderTest extends AnyFunSpec with Matchers {

  class BufferPrimitiveWriter extends RowBinaryClickHousePrimitiveTypeWriter {
    val buffer = new ByteArrayOutputStream()
    override def writeByte(b: Int): Unit = buffer.write(b)
  }

  case class DoubledInteger(value: Int)

  implicit val doubledIntegerEncoder: RowBinaryClickHouseRecordEncoder[DoubledInteger] =
    (t: DoubledInteger, pw: RowBinaryClickHousePrimitiveTypeWriter) => pw.writeInt32(t.value * 2)

  case class RepeatedString(value: String)

  implicit val repeatedStringEncoder: RowBinaryClickHouseRecordEncoder[RepeatedString] =
    (t: RepeatedString, pw: RowBinaryClickHousePrimitiveTypeWriter) => {
      val bytes = t.value.getBytes("UTF-8")
      pw.writeInt32(bytes.length * 2)
      pw.writeByteArray(bytes)
      pw.writeByteArray(bytes)
    }

  case class BasicRecord(a: Byte, b: Short, c: Int, d: Long, e: Char, f: String, g: Boolean)
  case class ComplexRecord(float: Float, double: Double, time: Timestamp, array: Array[Byte])
  case class OptionalRecord(a: Option[Int], b: Option[String])
  case class CompositeRecord(a: Array[Option[Int]], b: Array[Array[Int]])
  case class CustomRecord(i: DoubledInteger, s: RepeatedString)
  case class DecimalRecord(a: BigDecimal @DecimalEncoding(18, 10), b: BigDecimal @DecimalEncoding(20, 5))

  case class LengthAnnotatedRecord(
      a: String @FixedLength(5),
      b: Option[String] @MaxLength(5, truncate = false),
      c: Option[String] @FixedLength(5),
      d: Array[Byte] @FixedLength(5, truncate = true),
      e: Array[Byte] @FixedLength(5, truncate = true)
  )

  def encoderFor[T: RowBinaryClickHouseRecordEncoder]: RowBinaryClickHouseRecordEncoder[T] =
    implicitly[RowBinaryClickHouseRecordEncoder[T]]

  it("should write basic types") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[BasicRecord].write(BasicRecord(1, 2, 3, 4, 'a', "abcd", g = true), testWriter)

    expectedWriter.writeByte(1)
    expectedWriter.writeInt16(2)
    expectedWriter.writeInt32(3)
    expectedWriter.writeInt64(4)
    expectedWriter.writeByte('a')
    expectedWriter.writeString("abcd", maxBytes = 10, truncate = true)
    expectedWriter.writeByte(1)

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should write complex types") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[ComplexRecord].write(ComplexRecord(1.2f, 2.3, Timestamp(1554296316129L), Array(1, 2, 3, 4)), testWriter)

    expectedWriter.writeFloat32(1.2f)
    expectedWriter.writeFloat64(2.3)
    expectedWriter.writeDateTime(Timestamp(1554296316129L))
    expectedWriter.writeLeb128(4)
    expectedWriter.writeByteArray(Array[Byte](1, 2, 3, 4))

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should write optional types") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[OptionalRecord].write(OptionalRecord(Some(1), None), testWriter)

    expectedWriter.writeByte(0)
    expectedWriter.writeInt32(1)
    expectedWriter.writeByte(1)

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should write nested types") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[CompositeRecord].write(CompositeRecord(Array(Some(1), None), Array(Array(1), Array(2, 3))), testWriter)

    expectedWriter.writeLeb128(2L)
    expectedWriter.writeByte(0)
    expectedWriter.writeInt32(1)
    expectedWriter.writeByte(1)

    expectedWriter.writeLeb128(2)
    expectedWriter.writeLeb128(1)
    expectedWriter.writeInt32(1)
    expectedWriter.writeLeb128(2)
    expectedWriter.writeInt32(2)
    expectedWriter.writeInt32(3)

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should write custom types") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[CustomRecord].write(CustomRecord(DoubledInteger(1), RepeatedString("abc")), testWriter)

    expectedWriter.writeInt32(2)
    expectedWriter.writeInt32(6)
    expectedWriter.writeByteArray("abc".getBytes("UTF-8"))
    expectedWriter.writeByteArray("abc".getBytes("UTF-8"))

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should write decimal types") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[DecimalRecord].write(
      DecimalRecord(
        BigDecimal(12345.67890123),
        BigDecimal(123456789.123456789)
      ),
      testWriter
    )

    expectedWriter.writeInt64(123456789012300L)

    expectedWriter.writeInt64(0L)
    expectedWriter.writeInt64(12345678912345L)

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should fail writing decimal values that do not fit into the specified precision/scale") {
    val testWriter = new BufferPrimitiveWriter

    assertThrows[IllegalArgumentException] {
      encoderFor[DecimalRecord].write(
        DecimalRecord(
          BigDecimal(990000000), // does not fit into DECIMAL(18, 10)
          BigDecimal(123456789.123456789)
        ),
        testWriter
      )
    }
  }

  it("should correctly write length annotated records") {
    val (testWriter, expectedWriter) = (new BufferPrimitiveWriter, new BufferPrimitiveWriter)

    encoderFor[LengthAnnotatedRecord].write(
      LengthAnnotatedRecord(
        "1234567890",
        Some("123"),
        Some("456"),
        Array[Byte](1, 2, 3),
        Array[Byte](10, 11, 12, 13, 14, 15, 16)
      ),
      testWriter
    )

    expectedWriter.writeFixedString("12345", 5, truncate = true)
    expectedWriter.writeByte(0)
    expectedWriter.writeString("123", 5, truncate = true)
    expectedWriter.writeByte(0)
    expectedWriter.writeFixedString("456", 5, truncate = true)
    expectedWriter.writeLeb128(5)
    expectedWriter.writeFixedByteArray(Array[Byte](1, 2, 3), 5, truncate = true, padWith = 0)
    expectedWriter.writeLeb128(5)
    expectedWriter.writeFixedByteArray(Array[Byte](10, 11, 12, 13, 14, 15, 16), 5, truncate = true, padWith = 0)

    testWriter.buffer.toByteArray shouldEqual expectedWriter.buffer.toByteArray
  }

  it("should fail writing records with lengths exceeding annotated sizes when truncation is disabled") {
    val testWriter = new BufferPrimitiveWriter

    assertThrows[IllegalArgumentException] {
      encoderFor[LengthAnnotatedRecord].write(
        LengthAnnotatedRecord(
          "1234567890",
          Some("123456"), // too long
          Some("456"),
          Array[Byte](1, 2, 3),
          Array[Byte](10, 11, 12, 13, 14, 15, 16)
        ),
        testWriter
      )
    }
  }
}
