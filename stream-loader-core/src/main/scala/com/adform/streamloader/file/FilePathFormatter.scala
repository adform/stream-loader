/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import com.adform.streamloader.model.RecordRange

/**
  * Base trait used to construct file paths given ranges of records they contain.
  */
trait FilePathFormatter {

  /**
    * Constructs a file path given the ranges of records contained in the file.
    *
    * @param ranges Ranges of records in the file.
    * @return A relative path for the file.
    */
  def formatPath(ranges: Seq[RecordRange]): String
}
