/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse

import com.adform.streamloader.sink.file.{FileBuilder, FileBuilderFactory}
import com.clickhouse.client.ClickHouseFormat

/**
  * A FileBuilder able to build files that can be loaded to ClickHouse.
  *
  * @tparam R type of the records written to files being built.
  */
trait ClickHouseFileBuilder[-R] extends FileBuilder[R] {

  /**
    * The ClickHouse file format for the files being built.
    */
  def format: ClickHouseFormat
}

trait ClickHouseFileBuilderFactory[R] extends FileBuilderFactory[R, ClickHouseFileBuilder[R]] {
  def newFileBuilder(): ClickHouseFileBuilder[R]
}
