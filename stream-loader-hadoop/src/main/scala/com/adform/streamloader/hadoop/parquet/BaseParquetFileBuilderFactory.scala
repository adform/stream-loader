/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import java.io.File
import java.util.UUID

import com.adform.streamloader.file.{Compression, FileBuilderFactory}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Base class for parquet file builder factories.
  */
abstract class BaseParquetFileBuilderFactory[-R](compression: Compression) extends FileBuilderFactory[R] {

  protected def getFile(filenamePrefix: String): File = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    val filename = s"$filenamePrefix${UUID.randomUUID().toString}"
    new File(s"$tmpDir/$filename.parquet")
  }

  protected val compressionCodecName: CompressionCodecName = compression match {
    case Compression.NONE => CompressionCodecName.UNCOMPRESSED
    case Compression.ZSTD => CompressionCodecName.ZSTD
    case Compression.GZIP => CompressionCodecName.GZIP
    case Compression.LZOP => CompressionCodecName.LZO
    case Compression.SNAPPY => CompressionCodecName.SNAPPY
    case Compression.LZ4 => CompressionCodecName.LZ4
    case _ => throw new UnsupportedOperationException(s"Compression '$compression' is unsupported in parquet")
  }
}
