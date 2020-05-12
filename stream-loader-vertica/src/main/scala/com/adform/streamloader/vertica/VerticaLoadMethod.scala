/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica

sealed trait VerticaLoadMethod

object VerticaLoadMethod {

  case object AUTO extends VerticaLoadMethod
  case object DIRECT extends VerticaLoadMethod
  case object TRICKLE extends VerticaLoadMethod

  def parse(method: String): VerticaLoadMethod = method.toUpperCase match {
    case "AUTO" => AUTO
    case "DIRECT" => DIRECT
    case "TRICKLE" => TRICKLE
    case str => throw new IllegalArgumentException(s"Invalid vertica load method '$str'")
  }
}
