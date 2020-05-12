/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import com.adform.streamloader.model.Record

/**
  * A formatter for mapping source records being written to a file to `R` typed records.
  *
  * @tparam R Type of records written to the file.
  * @tparam F Type of the ID of the file that is being written to.
  */
trait FileRecordFormatter[+R, F] {

  /**
    * Constructs a sequence of records of type `R` that need to be written to a file from a source record.
    */
  def format(record: Record, fileId: F): Seq[R]
}

/**
  * A formatter for mapping source records to `R` typed records.
  *
  * @tparam R Type of records being formatted to.
  */
trait RecordFormatter[+R] {

  /**
    * Constructs a sequence of records of type `R` from a source record.
    */
  def format(record: Record): Seq[R]

  /**
    * Maps this formatter to a [[FileRecordFormatter]], ignoring the file ID.
    */
  def toFileRecordFormatter[F]: FileRecordFormatter[R, F] = (record: Record, _: F) => format(record)
}
