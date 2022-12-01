/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class FifoHashSetTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  describe("FifoHashSet") {

    it("should drop oldest when limit reached") {
      val set = FifoHashSet[String](10)

      for (i <- 1 to 14) {
        set.add(s"$i key")
      }

      set.contains("4 key") shouldBe false
      set.contains("4 key") shouldBe false
      set.contains("5 key") shouldBe true

      for (i <- 5 to 14) {
        set.contains(s"$i key") shouldBe true
      }
    }

    it("should move to top") {
      val set = FifoHashSet[String](5)
      set.add("1 key")
      set.add("2 key")
      set.add("3 key")
      set.add("1 key")

      set.iterator().next() shouldBe "1 key"
    }

    it("should show correct size") {
      val set = FifoHashSet[String](5)
      set.add("1 key")
      set.add("2 key")
      set.add("3 key")

      set.size() shouldBe 3

      set.clear()

      set.size() shouldBe 0
    }
  }
}
