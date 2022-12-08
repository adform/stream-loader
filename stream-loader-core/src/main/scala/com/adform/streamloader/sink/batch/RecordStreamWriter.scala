/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch

/**
  * An abstract writer for a stream of records of type `R`, most probably backed by some storage, e.g. a file.
  */
trait RecordStreamWriter[-R] {

  /**
    * Writes a header, if necessary.
    */
  def writeHeader(): Unit = {}

  /**
    * Writes a given record.
    */
  def writeRecord(record: R): Unit

  /**
    * Writes a footer, if necessary.
    */
  def writeFooter(): Unit = {}

  /**
    * Performs any necessary cleanup.
    */
  def close(): Unit = {}
}
