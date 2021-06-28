/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.csv

import java.io.OutputStream

import com.adform.streamloader.batch.RecordStreamWriter
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}

case class CsvFormat(columnSeparator: String, rowSeparator: String, includeHeader: Boolean, nullValue: String)

object CsvFormat {
  val DEFAULT: CsvFormat = CsvFormat(columnSeparator = "\t", rowSeparator = "\n", includeHeader = false, nullValue = "")
}

/**
  * A stream writer that encodes values of type `R` using an implicit [[CsvRecordEncoder]] and outputs
  * CSV content to the given output stream.
  *
  * @param os Output stream to write to.
  * @param format The CSV formatting options to use.
  *
  * @tparam R Type of the records being written.
  */
class CsvRecordStreamWriter[R: CsvRecordEncoder](os: OutputStream, format: CsvFormat = CsvFormat.DEFAULT)
    extends RecordStreamWriter[R] {

  private val encoder = implicitly[CsvRecordEncoder[R]]

  private val settings = new CsvWriterSettings()

  settings.setNullValue(format.nullValue)
  settings.getFormat.setDelimiter(format.columnSeparator)
  settings.getFormat.setLineSeparator(format.rowSeparator)

  private val writer = new CsvWriter(os, settings)

  override def writeHeader(): Unit = {
    if (format.includeHeader) {
      writer.writeHeaders(encoder.columnNames: _*)
    }
  }

  override def writeRecord(record: R): Unit = writer.writeRow(encoder.safeEncode(record).map(_.orNull))

  override def close(): Unit = writer.close()
}
