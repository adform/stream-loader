/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse

import java.io.File
import com.adform.streamloader.model.StreamRange
import com.adform.streamloader.sink.file.FileRecordBatch
import ru.yandex.clickhouse.domain.ClickHouseFormat

/**
  * A file containing a batch of records in some ClickHouse supported format that can be loaded to ClickHouse.
  */
case class ClickHouseFileRecordBatch(
    file: File,
    format: ClickHouseFormat,
    recordRanges: Seq[StreamRange],
    rowCount: Long
) extends FileRecordBatch
