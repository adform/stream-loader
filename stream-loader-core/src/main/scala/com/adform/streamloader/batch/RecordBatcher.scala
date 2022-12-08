/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch

/**
  * A record batcher that provides new record batch builders.
  *
  * @tparam B Type of record batches being built.
  */
trait RecordBatcher[+B <: RecordBatch] {

  /**
    * Gets a new record batch builder.
    */
  def newBatchBuilder(): RecordBatchBuilder[B]

  /**
    * Closes the batcher.
    */
  def close(): Unit = {}
}
