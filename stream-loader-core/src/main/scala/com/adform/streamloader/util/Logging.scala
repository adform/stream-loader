/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import org.log4s._

/**
  * Convenience trait for importing an instance of a logger.
  */
trait Logging {
  protected val log: Logger = getLogger(getClass)
}
