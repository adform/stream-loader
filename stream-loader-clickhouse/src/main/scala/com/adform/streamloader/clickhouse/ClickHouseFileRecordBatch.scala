/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse

import com.adform.streamloader.model.StreamRange
import com.adform.streamloader.sink.file.{Compression, FileRecordBatch}
import com.clickhouse.data.ClickHouseFormat

import java.io.File

/**
  * A file containing a batch of records in some ClickHouse supported format that can be loaded to ClickHouse.
  */
case class ClickHouseFileRecordBatch(
    file: File,
    format: ClickHouseFormat,
    fileCompression: Compression,
    recordRanges: Seq[StreamRange],
    rowCount: Long
) extends FileRecordBatch
