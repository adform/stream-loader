/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file.native

import java.io.File

import com.adform.streamloader.file.{Compression, StreamFileBuilder}
import com.adform.streamloader.vertica.VerticaLoadMethod
import com.adform.streamloader.vertica.file.VerticaFileBuilder

/**
  * File builder for the native Vertica file format, requires an implicit [[NativeVerticaRecordEncoder]] in scope.
  *
  * @param compression File compression to use.
  * @param bufferSizeBytes Size of the file write buffer.
  * @tparam R type of the records written to files being built.
  */
class NativeVerticaFileBuilder[-R: NativeVerticaRecordEncoder](
    compression: Compression,
    bufferSizeBytes: Int = 8192
) extends StreamFileBuilder[R](
      os => new NativeVerticaRecordStreamWriter[R](os),
      compression,
      bufferSizeBytes
    )
    with VerticaFileBuilder[R] {

  override def copyStatement(file: File, table: String, loadMethod: VerticaLoadMethod): String = {
    s"COPY $table FROM LOCAL '${file.getAbsolutePath}' ${compressionStr(compression)} NATIVE ABORT ON ERROR ${loadMethodStr(loadMethod)} NO COMMIT"
  }
}
