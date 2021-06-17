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
  * Base trait for defining a record partitioning strategy, e.g. by day or by country, etc.
  *
  * @tparam R Type of formatted records, i.e. the one being written to storage.
  * @tparam P Type of the partition.
  */
trait RecordPartitioner[-R, +P] {

  /**
    * Gets the partition this record belongs to.
    *
    * @param raw The original un-formatted record.
    * @param formatted The formatted record.
    * @return The partition the record should be routed to.
    */
  def partition(raw: Record, formatted: R): P
}
