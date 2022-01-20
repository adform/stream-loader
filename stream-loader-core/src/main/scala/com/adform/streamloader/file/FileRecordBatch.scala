/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.io.File

import com.adform.streamloader.model.{RecordBatch, RecordRange}

/**
  * Base trait for file based record batches.
  */
trait BaseFileRecordBatch extends RecordBatch {
  val file: File

  override def discard(): Boolean = file.delete()
}

case class FileRecordBatch(file: File, recordRanges: Seq[RecordRange]) extends BaseFileRecordBatch
