/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.io.File

/**
  * A data file builder that keeps adding records and returns the resulting file after flushing to disk.
  *
  * @tparam R type of the records being added.
  */
trait FileBuilder[-R] {

  /**
    * Adds a record to the file.
    */
  def write(record: R): Unit

  /**
    * Gets the current size of the data added to the file.
    */
  def getDataSize: Long

  /**
    * Gets the count of records currently written to the file.
    */
  def getRecordCount: Long

  /**
    * Builds a data file from all the added records and flushes it to disk.
    * The builder instance can no longer be used after calling this method.
    *
    * @return The resulting file if any records were added, None otherwise.
    */
  def build(): Option[File]

  /**
    * Discards the file currently being built and closes the builder.
    */
  def discard(): Unit
}

/**
  * A [[FileBuilder]] instance producer.
  *
  * @tparam R type of the records written to files being built.
  */
trait FileBuilderFactory[-R] {

  /**
    * Creates a new instance of a `FileBuilder`.
    */
  def newFileBuilder(): FileBuilder[R]
}
