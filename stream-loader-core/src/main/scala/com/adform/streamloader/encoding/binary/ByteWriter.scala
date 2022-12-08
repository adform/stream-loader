/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.binary

/**
  * A base trait that writes bytes to some underlying storage.
  */
trait ByteWriter {

  def writeByte(b: Int): Unit

  def writeBytes(bytes: Int*): Unit = {
    for (byte <- bytes) writeByte(byte)
  }

  /**
    * Writes a specified number of bytes from a given array and stops once the limit is reached.
    * Fails immediately if the byte array length exceeds `length` and `truncate` is false.
    */
  def writeByteArray(bytes: Array[Byte], maxLength: Int, truncate: Boolean = true): Unit = {
    if (bytes.length > maxLength && !truncate)
      throw new IllegalArgumentException(
        s"Byte array '${bytes.mkString(" ")}' occupies ${bytes.length} bytes and does not fit into $maxLength bytes"
      )

    var i = 0
    val len = Math.min(maxLength, bytes.length)
    while (i < len) {
      writeByte(bytes(i))
      i += 1
    }
  }

  def writeByteArray(bytes: Array[Byte]): Unit = writeByteArray(bytes, bytes.length)

  /**
    * Writes bytes from the given array and either pads it to the required `length` or truncates it if
    * the length of the array exceeds the required `length`.
    * Fails immediately if the byte array length exceeds `length` and `truncate` is false.
    */
  def writeFixedByteArray(bytes: Array[Byte], length: Int, truncate: Boolean, padWith: Byte): Unit = {
    if (bytes.length > length && !truncate)
      throw new IllegalArgumentException(
        s"Byte array '${bytes.mkString(" ")}' occupies ${bytes.length} bytes and does not fit into $length bytes"
      )

    writeByteArray(bytes, length)
    for (_ <- 0 until (length - bytes.length)) { // right pad
      writeByte(padWith)
    }
  }

  /**
    * Writes the UTF-8 byte representation of the given string. The string is truncated at the character boundary
    * to fit into `lengthBytes` bytes, if `truncate` is true. Padding bytes are written to make the total number
    * of bytes written equal to `lengthBytes`.
    * Fails immediately if the byte array length exceeds `length` and `truncate` is false.
    */
  protected def writeFixedString(s: String, lengthBytes: Int, truncate: Boolean, padWith: Byte): Unit = {
    val (bytes, truncatedLength) = stringToBytes(s, lengthBytes)

    if (bytes.length > lengthBytes && !truncate)
      throw new IllegalArgumentException(
        s"String '$s' occupies ${bytes.length} bytes and does not fit into $lengthBytes bytes"
      )

    writeByteArray(bytes, truncatedLength)
    for (_ <- 0 until (lengthBytes - truncatedLength)) { // right pad
      writeByte(padWith)
    }
  }

  /**
    * Converts a given string to it's UTF-8 byte representation and truncates it at the character
    * boundary if needed, so that it does not occupy more that the specified max bytes.
    *
    * @param s String to convert
    * @param maxBytes Max bytes to return
    * @return The full string byte representation and the number of bytes that fit into the max specified bytes.
    */
  protected def stringToBytes(s: String, maxBytes: Int): (Array[Byte], Int) = {
    val bytes = s.getBytes("UTF-8")
    val len = if (bytes.length <= maxBytes) {
      bytes.length
    } else {
      var idx = maxBytes // check the next byte after the limit
      if ((bytes(idx) & 0x80) == 0) { // is the high bit 0?
        maxBytes // if so, it's a single byte char, so it's safe to cut it off
      } else if ((bytes(idx) & 0xc0) == 0xc0) { // are the two high bits 1?
        maxBytes // if so, it's the start of a multi-byte char, so it's safe to cut
      } else {
        // the high bit is 1, but the next one is 0, so we're inside a multi-byte char,
        // we have to go back and find the start of the sequence, i.e. the byte with
        // two high bits, which is the start of this sequence and cut there
        do idx -= 1 while ((bytes(idx) & 0xc0) != 0xc0)
        idx
      }
    }
    (bytes, len)
  }
}
