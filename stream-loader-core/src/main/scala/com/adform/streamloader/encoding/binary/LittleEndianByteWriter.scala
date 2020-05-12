/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.binary

import java.nio.{ByteBuffer, ByteOrder}

/**
  * A byte writer with convenience methods for writing primitive types in little endian byte encoding.
  */
trait LittleEndianByteWriter extends ByteWriter {

  def writeInt16(i: Short): Unit = {
    writeByte((i >> 0) & 0xFF)
    writeByte((i >> 8) & 0xFF)
  }

  def writeInt32(i: Int): Unit = {
    writeByte(((i >> 0) & 0xFF))
    writeByte(((i >> 8) & 0xFF))
    writeByte(((i >> 16) & 0xFF))
    writeByte(((i >> 24) & 0xFF))
  }

  def writeInt64(i: Long): Unit = {
    writeByte(((i >> 0) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 8) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 16) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 24) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 32) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 40) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 48) & 0xFF).asInstanceOf[Int])
    writeByte(((i >> 56) & 0xFF).asInstanceOf[Int])
  }

  def writeFloat32(d: Float): Unit = {
    val bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    bb.putFloat(d)
    writeByteArray(bb.array())
  }

  def writeFloat64(d: Double): Unit = {
    val bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    bb.putDouble(d)
    writeByteArray(bb.array())
  }

  private val scaleMultipliers: Array[BigDecimal] = (0 to 1024).map(s => BigDecimal(10).pow(s)).toArray

  def writeDecimal(d: BigDecimal, precision: Int, scale: Int): Unit = {
    val size = math.ceil((((precision / 19) + 1) * 8).toDouble).toInt
    val scaledInt = (d * scaleMultipliers(scale)).toBigInt
    val bytes = scaledInt.toByteArray

    if (bytes.length > size)
      throw new IllegalArgumentException(s"Decimal '$d' does not fit into DECIMAL($precision, $scale)")

    // The implementation below is equivalent to the following, but does not copy/allocate new byte arrays:
    //
    // val paddedBytes = scaledInt.toByteArray.reverse.padTo(size, (if (d < 0) 255 else 0).asInstanceOf[Byte])
    // paddedBytes.grouped(8).toList.reverse.foreach(x => writeByteArray(x))

    val byteGroupCount = size / 8
    var byteGroup = 0

    while (byteGroup < byteGroupCount) {
      var byteIdx = bytes.length - (8 * (byteGroupCount - byteGroup - 1)) - 1
      val byteEndIdx = byteIdx - 8
      while (byteIdx > byteEndIdx) {
        if ((byteIdx > bytes.length) || byteIdx < 0) {
          if (d < 0) writeByte(255) else writeByte(0) // two's complement, so pad with 1 if negative
        } else {
          writeByte(bytes(byteIdx))
        }
        byteIdx -= 1
      }
      byteGroup += 1
    }
  }
}
