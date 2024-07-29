/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.UUID

object UuidExtensions {
  implicit class RichUuid(uuid: UUID) {
    def toBytes: Array[Byte] = {
      val bb = ByteBuffer.allocate(16)
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
      bb.array
    }
  }

  private val random = new SecureRandom

  def randomUUIDv7(): UUID = {
    val value = new Array[Byte](16)
    random.nextBytes(value)

    val timestamp = ByteBuffer.allocate(8)
    timestamp.putLong(System.currentTimeMillis)
    System.arraycopy(timestamp.array, 2, value, 0, 6)

    value(6) = ((value(6) & 0x0f) | 0x70).toByte
    value(8) = ((value(8) & 0x3f) | 0x80).toByte

    val buf = ByteBuffer.wrap(value)
    val high = buf.getLong
    val low = buf.getLong

    new UUID(high, low)
  }
}
