/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.util.Try

/**
  * A value wrapper for unix epoch milliseconds.
  */
case class Timestamp(millis: Long) extends AnyVal with Ordered[Timestamp] {

  /**
    * Formats the timestamp as a string.
    *
    * @param pattern A `DateTimeFormatter` compatible formatting pattern.
    * @return The timestamp formatted as a string.
    */
  def format(pattern: String): Try[String] = Try {
    require(millis >= 0, "millis must be greater or equal to 0")
    LocalDateTime
      .ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))
      .format(DateTimeFormatter.ofPattern(pattern))
  }

  override def compare(that: Timestamp): Int = millis.compare(that.millis)
}
