/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.source

import com.adform.streamloader.model.Timestamp
import com.adform.streamloader.util.TimeProvider
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Duration

class MaxWatermarkProviderTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class MockTimeProvider(var time: Long) extends TimeProvider {
    override def currentMillis: Long = time
  }

  it("should advance the watermark when time advances") {
    val wp = new MaxWatermarkProvider(Duration.ofHours(1))(new MockTimeProvider(0L))

    wp.observeEvent(Timestamp(1L))
    wp.currentWatermark shouldEqual Timestamp(1L)

    wp.observeEvent(Timestamp(2L))
    wp.currentWatermark shouldEqual Timestamp(2L)

    wp.observeEvent(Timestamp(3L))
    wp.currentWatermark shouldEqual Timestamp(3L)
  }

  it("should not change the watermark if late data arrives") {
    val wp = new MaxWatermarkProvider(Duration.ofHours(1))(new MockTimeProvider(0L))

    wp.observeEvent(Timestamp(10L))
    wp.currentWatermark shouldEqual Timestamp(10L)

    wp.observeEvent(Timestamp(2L))
    wp.currentWatermark shouldEqual Timestamp(10L)

    wp.observeEvent(Timestamp(3L))
    wp.currentWatermark shouldEqual Timestamp(10L)

    wp.observeEvent(Timestamp(11L))
    wp.currentWatermark shouldEqual Timestamp(11L)
  }

  it("should not advance the watermark if timestamp crosses valid threshold") {
    val tp = new MockTimeProvider(0L)
    val wp = new MaxWatermarkProvider(Duration.ofMillis(100))(tp)

    wp.observeEvent(Timestamp(1L))
    wp.currentWatermark shouldEqual Timestamp(1L)

    wp.observeEvent(Timestamp(101L))
    wp.currentWatermark shouldEqual Timestamp(1L)

    tp.time = 100L
    wp.observeEvent(Timestamp(101L))
    wp.currentWatermark shouldEqual Timestamp(101L)
  }
}
