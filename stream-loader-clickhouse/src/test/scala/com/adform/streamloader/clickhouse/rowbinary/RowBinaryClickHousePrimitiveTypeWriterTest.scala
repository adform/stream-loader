/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RowBinaryClickHousePrimitiveTypeWriterTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  private def longToLeb128(value: Long): Array[Byte] = {
    def rec(v: Long): Seq[Byte] = {
      val byte = (v & 0x7f).toByte
      val nextV = v >> 7
      val toEmit = if (nextV != 0) (byte | 0x80).toByte else byte
      toEmit +: (if (nextV != 0) rec(nextV) else Seq.empty)
    }
    rec(value).toArray
  }

  private def leb128ToLong(stream: ByteBuffer): Long =
    LazyList(0, 7).zip(stream.array()).map {
      case (shift, e) => (e & 0x7f).toLong << shift
    } reduce { _ | _ }

  class BufferPrimitiveWriter extends RowBinaryClickHousePrimitiveTypeWriter {
    val buffer = new ByteArrayOutputStream()

    override def writeByte(b: Int): Unit = buffer.write(b)

    def writtenStringAndLength(str: String, maxBytes: Int): (String, Long) = {
      val (_, expectedStrLength) = stringToBytes(str, maxBytes)
      val bytesUsedForStrLengthEncoding = longToLeb128(expectedStrLength).length
      val bytes = buffer.toByteArray
      val encodedLength = leb128ToLong(ByteBuffer.wrap(bytes.take(bytesUsedForStrLengthEncoding)))
      val encodedString = new String(bytes.drop(bytesUsedForStrLengthEncoding), "UTF-8")
      (encodedString, encodedLength)
    }

    def writtenString(str: String, maxBytes: Int): String = writtenStringAndLength(str, maxBytes)._1

    def writtenFixedString: String = new String(buffer.toByteArray, "UTF-8")
  }

  it("should always write the correct string byte length") {
    forAll { (str: String, maxBytes: Short) =>
      whenever(maxBytes > 0) {
        val pw = new BufferPrimitiveWriter() { writeString(str, maxBytes, truncate = true) }
        val (s, len) = pw.writtenStringAndLength(str, maxBytes)

        s.getBytes("UTF-8").length shouldEqual len
      }
    }
  }

  it("should always produce trimmed strings that fit to max bytes specified") {
    forAll { (str: String, maxBytes: Short) =>
      whenever(maxBytes > 0) {
        val pw = new BufferPrimitiveWriter() { writeString(str, maxBytes, truncate = true) }
        pw.writtenString(str, maxBytes).getBytes("UTF-8").length should be <= maxBytes.toInt
      }
    }
  }

  it("should always trim strings correctly") {
    forAll { (str: String, maxBytes: Short) =>
      whenever(maxBytes > 0) {
        val pw = new BufferPrimitiveWriter() { writeString(str, maxBytes, truncate = true) }
        str.startsWith(pw.writtenString(str, maxBytes)) shouldEqual true
      }
    }
  }

  it("should correctly trim simple example strings to max specified bytes") {
    val str = "123456789"
    val pw = new BufferPrimitiveWriter() { writeString(str, maxBytes = 5, truncate = true) }
    pw.writtenString(str, 5) shouldEqual "12345"
  }

  it("should correctly trim multi-byte character strings to max specified bytes") {
    val maxBytes = 5
    // 4 x 2 byte characters, two should remain
    val str1 = "\u0080\u0080\u0080\u0080"
    val pw2 = new BufferPrimitiveWriter() { writeString(str1, maxBytes, truncate = true) }
    pw2.writtenString(str1, maxBytes) shouldEqual "\u0080\u0080"

    // 2 x 4 byte characters, one should remain
    val str2 = "\uD800\uDC00\uD800\uDC00"
    val pw4 = new BufferPrimitiveWriter() { writeString(str2, maxBytes, truncate = true) }
    pw4.writtenString(str2, maxBytes) shouldEqual "\uD800\uDC00"
  }

  it("should correctly trim fixed length strings") {
    val pw = new BufferPrimitiveWriter() {
      writeFixedString("123456789", lengthBytes = 5, truncate = true)
    }
    pw.writtenFixedString shouldEqual "12345"
  }

  it("should correctly pad fixed length strings") {
    val pw = new BufferPrimitiveWriter() {
      writeFixedString("123456789", lengthBytes = 15, truncate = true)
    }
    pw.writtenFixedString shouldEqual "123456789\u0000\u0000\u0000\u0000\u0000\u0000"
  }

  it("should correctly trim fixed length byte arrays") {
    val pw = new BufferPrimitiveWriter() {
      writeFixedByteArray(Array[Byte](1, 2, 3, 4, 5, 6), length = 5, truncate = true, padWith = 0)
    }
    pw.buffer.toByteArray shouldEqual Array[Byte](1, 2, 3, 4, 5)
  }

  it("should correctly pad fixed length byte arrays") {
    val pw = new BufferPrimitiveWriter() {
      writeFixedByteArray(Array[Byte](1, 2, 3, 4), length = 10, truncate = true, padWith = 0)
    }
    pw.buffer.toByteArray shouldEqual Array[Byte](1, 2, 3, 4, 0, 0, 0, 0, 0, 0)
  }

  it("should fail if truncation is disabled and variable length strings are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() {
        writeString("123456", 5, truncate = false)
      }
    }
  }

  it("should fail if truncation is disabled and fixed length strings are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() {
        writeFixedString("123456", 5, truncate = false)
      }
    }
  }

  it("should fail if truncation is disabled and variable length byte arrays are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() {
        writeFixedByteArray(Array[Byte](1, 2, 3, 4), 3, truncate = false, padWith = 0)
      }
    }
  }

  it("should fail if truncation is disabled and fixed length byte arrays are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() {
        writeFixedByteArray(Array[Byte](1, 2, 3, 4), 3, truncate = false, padWith = 0)
      }
    }
  }
}
