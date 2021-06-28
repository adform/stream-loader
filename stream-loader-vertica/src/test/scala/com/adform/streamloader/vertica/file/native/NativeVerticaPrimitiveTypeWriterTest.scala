/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file.native

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NativeVerticaPrimitiveTypeWriterTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class BufferPrimitiveWriter extends NativeVerticaPrimitiveTypeWriter {
    val buffer = new ByteArrayOutputStream()
    override def writeByte(b: Int): Unit = buffer.write(b)

    def writtenVarStringAndLength: (String, Int) = {
      val bytes = buffer.toByteArray
      val encodedLength = ByteBuffer.wrap(bytes.take(4).reverse).getInt
      val encodedString = new String(bytes.drop(4), "UTF-8")
      (encodedString, encodedLength)
    }
    def writtenVarString: String = writtenVarStringAndLength._1
    def writtenFixedString: String = new String(buffer.toByteArray, "UTF-8")
  }

  it("should always write the correct string byte length") {
    forAll { (str: String, maxBytes: Short) =>
      whenever(maxBytes > 0) {
        val pw = new BufferPrimitiveWriter() { writeVarString(str, maxBytes, truncate = true) }
        val (s, len) = pw.writtenVarStringAndLength
        s.getBytes("UTF-8").length shouldEqual len
      }
    }
  }

  it("should always produce trimmed strings that fit to max bytes specified") {
    forAll { (str: String, maxBytes: Short) =>
      whenever(maxBytes > 0) {
        val pw = new BufferPrimitiveWriter() { writeVarString(str, maxBytes, truncate = true) }
        pw.writtenVarString.getBytes("UTF-8").length should be <= maxBytes.toInt
      }
    }
  }

  it("should always trim strings correctly") {
    forAll { (str: String, maxBytes: Short) =>
      whenever(maxBytes > 0) {
        val pw = new BufferPrimitiveWriter() { writeVarString(str, maxBytes, truncate = true) }
        str.startsWith(pw.writtenVarString) shouldEqual true
      }
    }
  }

  it("should correctly trim simple example strings to max specified bytes") {
    val pw = new BufferPrimitiveWriter() { writeVarString("123456789", maxBytes = 5, truncate = true) }
    pw.writtenVarString shouldEqual "12345"
  }

  it("should correctly trim multi-byte character strings to max specified bytes") {
    // 4 x 2 byte characters, two should remain
    val pw2 = new BufferPrimitiveWriter() { writeVarString("\u0080\u0080\u0080\u0080", maxBytes = 5, truncate = true) }
    pw2.writtenVarString shouldEqual "\u0080\u0080"

    // 2 x 4 byte characters, one should remain
    val pw4 = new BufferPrimitiveWriter() { writeVarString("\uD800\uDC00\uD800\uDC00", maxBytes = 5, truncate = true) }
    pw4.writtenVarString shouldEqual "\uD800\uDC00"
  }

  it("should correctly trim fixed length strings") {
    val pw = new BufferPrimitiveWriter() { writeFixedString("123456789", lengthBytes = 5, truncate = true) }
    pw.writtenFixedString shouldEqual "12345"
  }

  it("should correctly pad fixed length strings") {
    val pw = new BufferPrimitiveWriter() { writeFixedString("123456789", lengthBytes = 15, truncate = true) }
    pw.writtenFixedString shouldEqual "123456789      "
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
      new BufferPrimitiveWriter() { writeVarString("123456", 5, truncate = false) }
    }
  }

  it("should fail if truncation is disabled and fixed length strings are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() { writeFixedString("123456", 5, truncate = false) }
    }
  }

  it("should fail if truncation is disabled and variable length byte arrays are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() { writeVarByteArray(Array[Byte](1, 2, 3, 4), 3, truncate = false) }
    }
  }

  it("should fail if truncation is disabled and fixed length byte arrays are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferPrimitiveWriter() { writeFixedByteArray(Array[Byte](1, 2, 3, 4), 3, truncate = false, padWith = 0) }
    }
  }
}
