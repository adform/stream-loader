/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.encoding.binary

import com.adform.streamloader.sink.encoding.binary.ByteWriter

import java.io.ByteArrayOutputStream
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ByteWriterTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class BufferByteWriter extends ByteWriter {
    val buffer = new ByteArrayOutputStream()

    override def writeByte(b: Int): Unit = buffer.write(b)

    def writeFixedString(s: String, lengthBytes: Int, truncate: Boolean): Unit =
      writeFixedString(s, lengthBytes, truncate, padWith = ' ')
  }

  it("should correctly write bytes") {
    val pw = new BufferByteWriter() { writeBytes(1, 2, 3) }
    pw.buffer.toByteArray shouldEqual Array(1, 2, 3)
  }

  it("should correctly trim fixed length strings") {
    val pw = new BufferByteWriter() { writeFixedString("123456789", lengthBytes = 5, truncate = true) }
    pw.buffer.toByteArray shouldEqual "12345".getBytes("UTF-8")
  }

  it("should correctly trim and pad multi-byte character strings to max specified bytes") {
    // 4 x 2 byte characters, two should remain, padded with spaces
    val pw2 = new BufferByteWriter() { writeFixedString("\u0080\u0080\u0080\u0080", lengthBytes = 5, truncate = true) }
    pw2.buffer.toByteArray shouldEqual "\u0080\u0080 ".getBytes("UTF-8")

    // 2 x 4 byte characters, one should remain, padded with spaces
    val pw4 = new BufferByteWriter() { writeFixedString("\uD800\uDC00\uD800\uDC00", lengthBytes = 5, truncate = true) }
    pw4.buffer.toByteArray shouldEqual "\uD800\uDC00 ".getBytes("UTF-8")
  }

  it("should correctly pad fixed length strings") {
    val pw = new BufferByteWriter() { writeFixedString("123456789", lengthBytes = 15, truncate = true) }
    pw.buffer.toByteArray shouldEqual "123456789      ".getBytes("UTF-8")
  }

  it("should correctly trim fixed length byte arrays") {
    val pw = new BufferByteWriter() {
      writeFixedByteArray(Array[Byte](1, 2, 3, 4, 5, 6), length = 5, truncate = true, padWith = 0)
    }
    pw.buffer.toByteArray shouldEqual Array[Byte](1, 2, 3, 4, 5)
  }

  it("should correctly pad fixed length byte arrays") {
    val pw = new BufferByteWriter() {
      writeFixedByteArray(Array[Byte](1, 2, 3, 4), length = 10, truncate = true, padWith = 0)
    }
    pw.buffer.toByteArray shouldEqual Array[Byte](1, 2, 3, 4, 0, 0, 0, 0, 0, 0)
  }

  it("should fail if truncation is disabled and fixed length strings are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferByteWriter() { writeFixedString("123456", 5, truncate = false) }
    }
  }

  it("should fail if truncation is disabled and fixed length byte arrays are too long") {
    assertThrows[IllegalArgumentException] {
      new BufferByteWriter() { writeFixedByteArray(Array[Byte](1, 2, 3, 4), 3, truncate = false, padWith = 0) }
    }
  }
}
