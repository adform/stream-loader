/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

/**
  * A current time provider, used for mocking time in tests.
  */
trait TimeProvider {

  /**
    * Gets the current unix epoch milliseconds.
    */
  def currentMillis: Long
}

object TimeProvider {
  val system: TimeProvider = new TimeProvider {
    override def currentMillis: Long = System.currentTimeMillis()
  }
}
