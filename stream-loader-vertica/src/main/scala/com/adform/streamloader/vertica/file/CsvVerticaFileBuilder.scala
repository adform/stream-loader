/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file

import java.io.File
import com.adform.streamloader.sink.encoding.csv.{CsvFileBuilder, CsvFormat, CsvRecordEncoder}
import com.adform.streamloader.sink.file.Compression
import com.adform.streamloader.vertica.VerticaLoadMethod

/**
  * A CSV file builder factory that is also a [[VerticaFileBuilderFactory]].
  */
class CsvVerticaFileBuilder[-R: CsvRecordEncoder](
    compression: Compression,
    bufferSizeBytes: Int,
    format: CsvFormat = CsvFormat.DEFAULT
) extends CsvFileBuilder(compression, bufferSizeBytes, format)
    with VerticaFileBuilder[R] {

  override def copyStatement(file: File, table: String, loadMethod: VerticaLoadMethod): String = {
    val skipHeader = if (format.includeHeader) "SKIP 1" else ""
    val delimiter = format.columnSeparator match {
      case "\t" => "E'\\t'"
      case x => s"'$x'"
    }
    s"COPY $table FROM LOCAL '${file.getAbsolutePath}' ${compressionStr(compression)} DELIMITER $delimiter $skipHeader ABORT ON ERROR ${loadMethodStr(loadMethod)} NO COMMIT"
  }
}
