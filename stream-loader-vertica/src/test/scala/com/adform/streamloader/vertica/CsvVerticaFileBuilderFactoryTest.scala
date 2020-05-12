/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import java.io.File

import com.adform.streamloader.encoding.csv.CsvFormat
import com.adform.streamloader.file.Compression
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CsvVerticaFileBuilderFactoryTest extends AnyFunSpec with Matchers {

  case class ExampleRecord(id: Int, name: String)

  it("should produce correct COPY statements") {
    val factory = new CsvVerticaFileBuilderFactory[ExampleRecord](
      Compression.ZSTD,
      bufferSizeBytes = 1024,
      VerticaLoadMethod.AUTO,
      CsvFormat(columnSeparator = ";", rowSeparator = "\n", includeHeader = true, nullValue = "\\N")
    )

    factory.copyStatement("table", new File("/tmp/test.zst")) shouldEqual
      "COPY table FROM LOCAL '/tmp/test.zst' ZSTD DELIMITER ';' SKIP 1 ABORT ON ERROR AUTO NO COMMIT"
  }
}
