/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.csv

import com.adform.streamloader.file.{Compression, StreamFileBuilderFactory}

/**
  * A CSV file builder factory.
  *
  * @param compression Compression to use.
  * @param bufferSizeBytes Buffer size when writing to files.
  * @param format CSV format settings.
  *
  * @tparam R Type of the records written to files being built.
  *           An implicit [[CsvRecordEncoder]] for the type must be available.
  *
  */
class CsvFileBuilderFactory[-R: CsvRecordEncoder](
    compression: Compression,
    bufferSizeBytes: Int = 4096,
    format: CsvFormat = CsvFormat.DEFAULT
) extends StreamFileBuilderFactory[R](os => new CsvRecordStreamWriter[R](os, format), compression, bufferSizeBytes)
