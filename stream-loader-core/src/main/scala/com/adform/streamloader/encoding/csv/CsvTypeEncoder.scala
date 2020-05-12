/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.csv

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

import com.adform.streamloader.model.Timestamp

/**
  * An encoder from some data type `T` to string.
  * Used to customize data type encodings in macro derived [[CsvRecordEncoder]] instances.
  */
trait CsvTypeEncoder[-T] {

  /**
    * Converts the given value to a string.
    */
  def encode(t: T): String
}

object CsvTypeEncoder {
  def timestampEncoder(format: String): CsvTypeEncoder[Timestamp] =
    (t: Timestamp) =>
      LocalDateTime
        .ofInstant(Instant.ofEpochMilli(t.millis), ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern(format))

  implicit val boolEncoder: CsvTypeEncoder[Boolean] = (f: Boolean) => f.toString
  implicit val floatEncoder: CsvTypeEncoder[Float] = (f: Float) => f.toString
  implicit val doubleEncoder: CsvTypeEncoder[Double] = (f: Double) => f.toString

  implicit val defaultTimestampEncoder: CsvTypeEncoder[Timestamp] = timestampEncoder("yyyy-MM-dd HH:mm:ss.SSS")
  implicit val uuidEncoder: CsvTypeEncoder[UUID] = (u: UUID) => u.toString
}
