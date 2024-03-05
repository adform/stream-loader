/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import javax.sql.DataSource

import scala.util.Using

trait JdbcStorageBackend {
  def dataSource: DataSource

  def executeStatement(query: String): Unit = {
    Using.resource(dataSource.getConnection()) { connection =>
      Using.resource(connection.prepareStatement(query)) { statement =>
        statement.execute()
      }
    }
  }
}
