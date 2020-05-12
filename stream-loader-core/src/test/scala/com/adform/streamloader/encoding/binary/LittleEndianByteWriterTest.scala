/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.binary

import java.io.ByteArrayOutputStream

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class LittleEndianByteWriterTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class BufferByteWriter extends LittleEndianByteWriter {
    val buffer = new ByteArrayOutputStream()
    override def writeByte(b: Int): Unit = buffer.write(b)
  }

  it("should correctly write int16") {
    val pw = new BufferByteWriter() { writeInt16(6666) }
    pw.buffer.toByteArray shouldEqual Array(10, 26)
  }

  it("should correctly write int32") {
    val pw = new BufferByteWriter() { writeInt32(66667777) }
    pw.buffer.toByteArray shouldEqual Array(1, 69, -7, 3)
  }

  it("should correctly write int64") {
    val pw = new BufferByteWriter() { writeInt64(66668888888L) }
    pw.buffer.toByteArray shouldEqual Array(56, -125, -58, -123, 15, 0, 0, 0)
  }

  it("should correctly write float32") {
    val pw = new BufferByteWriter() { writeFloat32(12.3456f) }
    pw.buffer.toByteArray shouldEqual Array(-108, -121, 69, 65)
  }

  it("should correctly write float64") {
    val pw = new BufferByteWriter() { writeFloat64(12.3456) }
    pw.buffer.toByteArray shouldEqual Array(-59, -2, -78, 123, -14, -80, 40, 64)
  }

  it("should correctly write decimals") {
    val pw = new BufferByteWriter() { writeDecimal(BigDecimal("1234.56789"), 10, 8) }
    pw.buffer.toByteArray shouldEqual Array(8, 26, -103, -66, 28, 0, 0, 0)
  }
}
