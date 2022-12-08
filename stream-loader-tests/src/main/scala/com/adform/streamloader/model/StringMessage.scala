/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import org.scalacheck.{Arbitrary, Gen}

case class StringMessage(content: String) extends StorageMessage {
  def getBytes: Array[Byte] = content.getBytes()
}

object StringMessage {
  val arbMessage: Arbitrary[StringMessage] = Arbitrary(
    Gen.alphaNumStr.suchThat(_.length < 500).map(StringMessage.apply)
  )
}
