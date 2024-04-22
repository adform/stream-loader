/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.nio.ByteBuffer
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
}
