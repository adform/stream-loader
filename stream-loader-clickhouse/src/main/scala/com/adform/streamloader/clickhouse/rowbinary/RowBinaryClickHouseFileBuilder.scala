/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import com.adform.streamloader.clickhouse.ClickHouseFileBuilder
import com.adform.streamloader.sink.file.{Compression, StreamFileBuilder}
import com.clickhouse.data.{ClickHouseCompression, ClickHouseFormat}

/**
  * File builder for the ClickHouse native RowBinary file format, requires
  * an implicit [[RowBinaryClickHouseRecordEncoder]] in scope.
  *
  * @param bufferSizeBytes Size of the file write buffer.
  *
  * @tparam R type of the records written to files being built.
  */
class RowBinaryClickHouseFileBuilder[-R: RowBinaryClickHouseRecordEncoder](
    fileCompression: Compression = Compression.NONE,
    bufferSizeBytes: Int = 8192
) extends StreamFileBuilder[R](
      os => new RowBinaryClickHouseRecordStreamWriter[R](os),
      fileCompression,
      bufferSizeBytes
    )
    with ClickHouseFileBuilder[R] {

  override val format: ClickHouseFormat = ClickHouseFormat.RowBinary

  override def compression: ClickHouseCompression = fileCompression match {
    case Compression.NONE => ClickHouseCompression.NONE
    case Compression.ZSTD => ClickHouseCompression.ZSTD
    case Compression.GZIP => ClickHouseCompression.GZIP
    case Compression.BZIP => ClickHouseCompression.BZ2
    case Compression.LZ4 => ClickHouseCompression.LZ4
    case _ => throw new UnsupportedOperationException(s"Compression $fileCompression is not supported by ClickHouse")
  }
}
