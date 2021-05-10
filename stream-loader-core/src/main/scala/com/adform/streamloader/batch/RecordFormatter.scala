/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch

import com.adform.streamloader.model.Record

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
}
