/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import java.io.File
import com.adform.streamloader.sink.encoding.csv.CsvFormat
import com.adform.streamloader.sink.file.Compression
import com.adform.streamloader.vertica.file.CsvVerticaFileBuilder
//import com.adform.streamloader.vertica.file.CsvVerticaFileBuilderFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CsvVerticaFileBuilderTest extends AnyFunSpec with Matchers {

  case class ExampleRecord(id: Int, name: String)

  it("should produce correct COPY statements") {
    val builder = new CsvVerticaFileBuilder[ExampleRecord](
      Compression.ZSTD,
      bufferSizeBytes = 1024,
      CsvFormat(columnSeparator = ";", rowSeparator = "\n", includeHeader = true, nullValue = "\\N")
    )

    builder.copyStatement(new File("/tmp/test.zst"), "table", VerticaLoadMethod.AUTO) shouldEqual
      "COPY table FROM LOCAL '/tmp/test.zst' ZSTD DELIMITER ';' SKIP 1 ABORT ON ERROR AUTO NO COMMIT"
  }
}
