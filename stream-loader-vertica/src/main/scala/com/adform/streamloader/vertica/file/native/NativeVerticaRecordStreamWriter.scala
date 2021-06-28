/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file.native

import java.io.{ByteArrayOutputStream, OutputStream}

import com.adform.streamloader.batch.RecordStreamWriter

/**
  * Stream writer implementation that encodes records using an implicit [[NativeVerticaRecordEncoder]]
  * and writes them to the provided output stream.
  * See the native format specification for more details:
  * https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm
  */
class NativeVerticaRecordStreamWriter[R: NativeVerticaRecordEncoder](os: OutputStream) extends RecordStreamWriter[R] {

  private val recordEncoder = implicitly[NativeVerticaRecordEncoder[R]]

  private val pw = new NativeVerticaPrimitiveTypeWriter {
    override def writeByte(b: Int): Unit = os.write(b)
  }

  private val columnCount = recordEncoder.staticColumnSizes.length
  private val nullBits = new Array[Byte](columnCount / 8 + (if (columnCount % 8 == 0) 0 else 1))

  private val buffer = new ByteArrayOutputStream(32768)
  private val bufferPw = new NativeVerticaPrimitiveTypeWriter {
    override def writeByte(b: Int): Unit = buffer.write(b)
  }

  private def setNullBit(columnIndex: Int): Unit = {
    nullBits(columnIndex / 8) = (nullBits(columnIndex / 8) | (1 << (8 - (columnIndex % 8) - 1))).asInstanceOf[Byte]
  }

  override def writeHeader(): Unit = {
    pw.writeBytes(0x4E, 0x41, 0x54, 0x49, 0x56, 0x45, 0x0A, 0xFF, 0x0D, 0x0A, 0x00) // magic bytes
    pw.writeInt32(2 + 1 + 2 + 4 * columnCount) // size of header = version + filler + column count (short) + column sizes (ints)
    pw.writeBytes(0x01, 0x00) // version
    pw.writeByte(0x00) // filler
    pw.writeInt16(columnCount.asInstanceOf[Short]) // column count
    recordEncoder.staticColumnSizes.foreach(pw.writeInt32) // column sizes
  }

  override def writeRecord(record: R): Unit = {
    buffer.reset()
    recordEncoder.write(record, bufferPw) // write record bytes to buffer
    pw.writeInt32(buffer.size()) // write record size to output

    java.util.Arrays.fill(nullBits, 0.asInstanceOf[Byte])
    recordEncoder.setNullBits(record, setNullBit) // set null bits
    pw.writeByteArray(nullBits) // write null bits to output

    buffer.writeTo(os) // dump buffer to output
  }

  override def close(): Unit = os.close()
}
