/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import java.nio.{ByteBuffer, ByteOrder}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import com.adform.streamloader.encoding.binary.LittleEndianByteWriter
import com.adform.streamloader.model.Timestamp

/**
  * Implementations for writing primitive data types in the ClickHouse native RowBinary encoding.
  */
trait RowBinaryClickHousePrimitiveTypeWriter extends LittleEndianByteWriter {

  def writeLeb128(i: Long): Unit = {
    var v = i
    do {
      val byte = (v & 0x7f).toByte
      val next = v >> 7
      writeByte(if (next != 0) (byte | 0x80).toByte else byte)
      v = next
    } while (v != 0)
  }

  // String is represented as a varint length (unsigned LEB128), followed by the bytes of the string.
  def writeString(s: String): Unit = {
    val (bytes, length) = stringToBytes(s, Int.MaxValue)
    writeLeb128(length)
    writeByteArray(bytes, length)
  }

  def writeString(s: String, maxBytes: Int, truncate: Boolean): Unit = {
    val (bytes, truncatedLength) = stringToBytes(s, maxBytes)

    if (bytes.length > maxBytes && !truncate)
      throw new IllegalArgumentException(
        s"String '$s' occupies ${bytes.length} bytes and does not fit into $maxBytes bytes"
      )

    writeLeb128(truncatedLength)
    writeByteArray(bytes, truncatedLength)
  }

  def writeFixedString(s: String, lengthBytes: Int, truncate: Boolean): Unit =
    writeFixedString(s, lengthBytes, truncate, padWith = 0)

  def writeDateTime(t: Timestamp): Unit = {
    writeInt32((t.millis / 1000).asInstanceOf[Int])
  }

  def writeDateTime(t: LocalDateTime): Unit = {
    writeInt32(t.toEpochSecond(ZoneOffset.UTC).asInstanceOf[Int])
  }

  def writeDate(t: LocalDate): Unit = {
    writeInt16(t.toEpochDay.asInstanceOf[Short])
  }

  def writeUuid(uuid: UUID): Unit = {
    val bb: ByteBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    writeByteArray(bb.array)
  }
}
