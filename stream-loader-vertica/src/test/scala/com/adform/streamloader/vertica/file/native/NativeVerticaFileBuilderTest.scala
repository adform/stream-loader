/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file.native

import java.io.File

import com.adform.streamloader.file.Compression
import com.adform.streamloader.vertica.VerticaLoadMethod
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class NativeVerticaFileBuilderTest extends AnyFunSpec with Matchers {

  case class ExampleRecord(id: Int, name: String)

  it("should produce correct COPY statements") {
    val builder = new NativeVerticaFileBuilder[ExampleRecord](Compression.ZSTD)

    builder.copyStatement(new File("/tmp/test.zst"), "table", VerticaLoadMethod.AUTO) shouldEqual
      "COPY table FROM LOCAL '/tmp/test.zst' ZSTD NATIVE ABORT ON ERROR AUTO NO COMMIT"
  }
}
