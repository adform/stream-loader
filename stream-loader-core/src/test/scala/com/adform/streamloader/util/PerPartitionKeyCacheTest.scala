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

class PerPartitionKeyCacheTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  describe("PerPartitionKeyCache") {
    it("should be ready when marked as ready") {
      val cache = KeyCache.perPartition[String](10)
      cache.assignPartition(1, 110, ready = true)

      cache.verifyAndSwitchIfReady(1, 10) shouldBe true
    }

    it("should not be ready when marked as not ready") {
      val cache = KeyCache.perPartition[String](10)
      cache.assignPartition(1, 110, ready = false)

      cache.verifyAndSwitchIfReady(1, 10) shouldBe false
    }
  }
}
