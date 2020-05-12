/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.native

import java.nio.{ByteBuffer, ByteOrder}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import com.adform.streamloader.encoding.binary.LittleEndianByteWriter
import com.adform.streamloader.model.Timestamp

/**
  * Implementations for writing primitive data types in the Vertica native file encoding.
  */
trait NativeVerticaPrimitiveTypeWriter extends LittleEndianByteWriter {

  private def writeTimestampMillis(epochMillis: Long): Unit = {
    writeInt64((epochMillis - 946684800000L) * 1000) // microseconds since 2000-01-01 UTC
  }

  def writeTimestamp(t: Timestamp): Unit = {
    writeTimestampMillis(t.millis)
  }

  def writeTimestamp(t: LocalDateTime): Unit = {
    writeTimestampMillis(t.toInstant(ZoneOffset.UTC).toEpochMilli)
  }

  def writeDate(t: LocalDate): Unit = {
    writeInt64(t.toEpochDay - 10957) // days since 2000-01-01
  }

  def writeVarString(s: String, maxBytes: Int, truncate: Boolean): Unit = {
    val (bytes, truncatedLength) = stringToBytes(s, maxBytes)

    if (bytes.length > maxBytes && !truncate)
      throw new IllegalArgumentException(
        s"String '$s' occupies ${bytes.length} bytes and does not fit into $maxBytes bytes")

    writeInt32(truncatedLength)
    writeByteArray(bytes, truncatedLength)
  }

  def writeFixedString(s: String, lengthBytes: Int, truncate: Boolean): Unit =
    writeFixedString(s, lengthBytes, truncate, padWith = ' ')

  def writeVarByteArray(bytes: Array[Byte], maxLength: Int, truncate: Boolean): Unit = {
    if (bytes.length > maxLength && !truncate)
      throw new IllegalArgumentException(
        s"Byte array '${bytes.mkString(" ")}' occupies ${bytes.length} bytes and does not fit into $maxLength bytes")

    val len = Math.min(bytes.length, maxLength)
    writeInt32(len)
    writeByteArray(bytes, len)
  }

  def writeUUID(t: UUID): Unit = {
    val bb = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
    bb.putLong(t.getMostSignificantBits)
    bb.putLong(t.getLeastSignificantBits)
    writeByteArray(bb.array)
  }
}
