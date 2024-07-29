/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import com.adform.streamloader.util.UuidExtensions.randomUUIDv7
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class UuidExtensionsTest extends AnyFunSpec with Matchers {

  it("should generate unique UUIDv7 values") {
    val uuids = (0 until 100).map(_ => randomUUIDv7())
    uuids should contain theSameElementsInOrderAs uuids.distinct
  }

  it("should generate alphabetically sorted UUIDv7 values") {
    val uuids = (0 until 10)
      .map(_ => {
        randomUUIDv7()
        Thread.sleep(5) // the v7 spec guarantees ordering in a 1ms scope
      })
      .map(_.toString)

    uuids should contain theSameElementsInOrderAs uuids.sorted
  }
}
