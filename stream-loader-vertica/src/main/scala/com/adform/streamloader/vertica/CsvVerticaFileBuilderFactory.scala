/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import java.io.File

import com.adform.streamloader.encoding.csv.{CsvFileBuilderFactory, CsvFormat, CsvRecordEncoder}
import com.adform.streamloader.file.Compression

/**
  * A CSV file builder that is also a [[VerticaCopyStatementProvider]].
  */
class CsvVerticaFileBuilderFactory[-R: CsvRecordEncoder](
    compression: Compression,
    bufferSizeBytes: Int,
    loadMethod: VerticaLoadMethod,
    format: CsvFormat = CsvFormat.DEFAULT
) extends CsvFileBuilderFactory(compression, bufferSizeBytes, format)
    with VerticaFileBuilderFactory[R] {

  override def copyStatement(table: String, file: File): String = {
    val skipHeader = if (format.includeHeader) "SKIP 1" else ""
    val delimiter = format.columnSeparator match {
      case "\t" => "E'\\t'"
      case x => s"'$x'"
    }
    s"COPY $table FROM LOCAL '${file.getAbsolutePath}' ${compressionStr(compression)} DELIMITER $delimiter $skipHeader ABORT ON ERROR ${loadMethodStr(loadMethod)} NO COMMIT"
  }
}
