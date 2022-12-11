/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import java.time.Duration

/**
  * Represents an interval in a single stream partition. Can be either an offset interval, a watermark difference
  * or some combination of both.
  */
sealed trait StreamInterval {
  def isZero: Boolean
}

object StreamInterval {

  /**
    * Represents a fixed offset difference.
    */
  case class OffsetRange(offset: Long) extends StreamInterval {
    override def isZero: Boolean = offset == 0
  }

  /**
    * Represents a difference in terms of the watermark.
    */
  case class WatermarkRange(duration: Duration) extends StreamInterval {
    override def isZero: Boolean = duration.isZero
  }
}
