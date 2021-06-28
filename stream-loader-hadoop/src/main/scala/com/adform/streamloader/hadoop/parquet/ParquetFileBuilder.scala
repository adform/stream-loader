/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import java.io.File

import com.adform.streamloader.file.FileBuilder
import org.apache.parquet.hadoop.ParquetWriter

/**
  * File builder implementation for parquet files, requires a parquet writer.
  */
class ParquetFileBuilder[-R](file: File, parquetWriter: ParquetWriter[R]) extends FileBuilder[R] {

  private var isClosed = false
  private var recordsWritten = 0L

  override def write(record: R): Unit = {
    recordsWritten += 1
    parquetWriter.write(record)
  }

  override def getRecordCount: Long = recordsWritten
  override def getDataSize: Long = parquetWriter.getDataSize

  override def build(): Option[File] = {
    if (!isClosed) {
      parquetWriter.close()
      isClosed = true
      if (recordsWritten > 0) Some(file) else None
    } else {
      None
    }
  }

  override def discard(): Unit = if (!isClosed) {
    parquetWriter.close()
    isClosed = true
  }
}
