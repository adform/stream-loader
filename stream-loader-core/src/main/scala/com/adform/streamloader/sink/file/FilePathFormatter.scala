/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

import com.adform.streamloader.model.StreamRange

/**
  * Base trait used to construct file paths when storing files to persistent storages.
  */
trait FilePathFormatter[-P] {

  /**
    * Constructs a file path given the partition and ranges of records contained in the file.
    *
    * @param partition The partition of records in the file.
    * @param recordRanges Ranges of records in the file.
    * @return A relative path for the file.
    */
  def formatPath(partition: P, recordRanges: Seq[StreamRange]): String
}
