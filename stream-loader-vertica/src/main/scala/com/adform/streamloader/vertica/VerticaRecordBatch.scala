/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

import com.adform.streamloader.batch.RecordBatch

/**
  * A record batch that can be loaded to Vertica, implementers must define a COPY statement generator method.
  */
trait VerticaRecordBatch extends RecordBatch {

  /**
    * Generates a COPY statement to load the batch to a destination table.
    *
    * @param table Vertica table to load data into.
    * @return A COPY statement.
    */
  def copyStatement(table: String): String
}
