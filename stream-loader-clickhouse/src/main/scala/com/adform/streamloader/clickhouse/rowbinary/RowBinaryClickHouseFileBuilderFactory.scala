/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import com.adform.streamloader.file.{Compression, StreamFileBuilderFactory}

/**
  * File builder factory for the ClickHouse native RowBinary file format, requires
  * an implicit [[RowBinaryClickHouseRecordEncoder]] in scope.
  *
  * @param bufferSizeBytes Size of the file write buffer.
  *
  * @tparam R type of the records written to files being built.
  */
class RowBinaryClickHouseFileBuilderFactory[-R: RowBinaryClickHouseRecordEncoder](
    bufferSizeBytes: Int = 8192,
) extends StreamFileBuilderFactory[R](
      os => new RowBinaryClickHouseRecordStreamWriter[R](os),
      Compression.NONE,
      bufferSizeBytes
    )
