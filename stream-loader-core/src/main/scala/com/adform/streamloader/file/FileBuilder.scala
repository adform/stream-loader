/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.io.File
import java.time.Duration

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
    * Gets the duration the file has already been open for.
    */
  def getOpenDuration: Duration

  /**
    * Builds a data file from all the added records and flushes it to disk.
    * The builder instance can no longer be used after calling this method.
    *
    * @return The resulting file if any records were added, None otherwise.
    */
  def build(): Option[File]
}

/**
  * A [[FileBuilder]] instance producer.
  *
  * @tparam R type of the records written to files being built.
  */
trait FileBuilderFactory[-R] {

  /**
    * Creates a new instance of a `FileBuilder`.
    *
    * @param filenamePrefix Prefix for all created files.
    */
  def newFileBuilder(filenamePrefix: String): FileBuilder[R]
}
