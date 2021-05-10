/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import org.json4s.JValue

/**
  * A JSON serializer for arbitrary types.
  */
trait JsonSerializer[S] {
  def serialize(value: S): JValue
  def deserialize(json: JValue): S
}
