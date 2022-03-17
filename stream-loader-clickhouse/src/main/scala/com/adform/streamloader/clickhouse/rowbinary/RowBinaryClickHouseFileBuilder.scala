/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import com.adform.streamloader.clickhouse.ClickHouseFileBuilder
import com.adform.streamloader.file.{Compression, StreamFileBuilder}
import ru.yandex.clickhouse.domain.ClickHouseFormat

/**
  * File builder for the ClickHouse native RowBinary file format, requires
  * an implicit [[RowBinaryClickHouseRecordEncoder]] in scope.
  *
  * @param bufferSizeBytes Size of the file write buffer.
  *
  * @tparam R type of the records written to files being built.
  */
class RowBinaryClickHouseFileBuilder[-R: RowBinaryClickHouseRecordEncoder](
    bufferSizeBytes: Int = 8192,
) extends StreamFileBuilder[R](
      os => new RowBinaryClickHouseRecordStreamWriter[R](os),
      Compression.NONE,
      bufferSizeBytes
    )
    with ClickHouseFileBuilder[R] {

  override val format: ClickHouseFormat = ClickHouseFormat.RowBinary
}
