/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file

import java.io.File

import com.adform.streamloader.file.{Compression, FileBuilder, FileBuilderFactory}
import com.adform.streamloader.vertica.VerticaLoadMethod

/**
  * A file builder that additionally knows how to generate
  * COPY statements to load resulting files to Vertica tables.
  *
  * @tparam R type of the records written to files being built.
  */
trait VerticaFileBuilder[-R] extends FileBuilder[R] {

  /**
    * Generates a COPY statement to load a given file to the destination table.
    *
    * @param file       File being loaded to.
    * @param table      Vertica table to load data into.
    * @param loadMethod The Vertica load method to use in the `COPY` statement.
    * @return A COPY statement.
    */
  def copyStatement(file: File, table: String, loadMethod: VerticaLoadMethod): String

  protected def compressionStr(compression: Compression): String = compression match {
    case Compression.NONE => ""
    case Compression.ZSTD => "ZSTD"
    case Compression.GZIP => "GZIP"
    case Compression.BZIP => "BZIP"
    case Compression.LZOP => "LZO"
    case _ => throw new UnsupportedOperationException(s"Compression $compression is not supported in Vertica")
  }

  protected def loadMethodStr(loadMethod: VerticaLoadMethod): String = loadMethod match {
    case VerticaLoadMethod.AUTO => "AUTO"
    case VerticaLoadMethod.DIRECT => "DIRECT"
    case VerticaLoadMethod.TRICKLE => "TRICKLE"
  }
}

trait VerticaFileBuilderFactory[R] extends FileBuilderFactory[R, VerticaFileBuilder[R]] {
  def newFileBuilder(): VerticaFileBuilder[R]
}
