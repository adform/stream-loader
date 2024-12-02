/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamRangeTest extends AnyFunSpec with Matchers {

  it("should merge ranges from the same topic partition") {
    val prev = StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(2, Timestamp(2)))
    val next = StreamRange("topic", 0, StreamPosition(2, Timestamp(2)), StreamPosition(4, Timestamp(4)))
    val merged = StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(4, Timestamp(4)))

    StreamRange.merge(prev, next) shouldEqual merged
  }

  it("should throw when attempting to merge ranges from different topic partitions") {
    val prev = StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(2, Timestamp(2)))
    val next = StreamRange("topic", 1, StreamPosition(2, Timestamp(2)), StreamPosition(4, Timestamp(4)))

    assertThrows[AssertionError](StreamRange.merge(prev, next))
  }

  it("should merge sequence of ranges correctly when topics and partitions overlap") {
    val prev = Seq(
      StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(2, Timestamp(2))),
      StreamRange("topic", 1, StreamPosition(10, Timestamp(10)), StreamPosition(20, Timestamp(20)))
    )
    val next = Seq(
      StreamRange("topic", 0, StreamPosition(2, Timestamp(2)), StreamPosition(4, Timestamp(4))),
      StreamRange("topic", 1, StreamPosition(20, Timestamp(20)), StreamPosition(40, Timestamp(40)))
    )

    val merged = Seq(
      StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(4, Timestamp(4))),
      StreamRange("topic", 1, StreamPosition(10, Timestamp(10)), StreamPosition(40, Timestamp(40)))
    )

    StreamRange.merge(prev, next) should contain theSameElementsAs merged
  }

  it("should merge sequence of ranges correctly when topics and partitions do not overlap") {
    val prev = Seq(
      StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(2, Timestamp(2)))
    )
    val next = Seq(
      StreamRange("topic", 1, StreamPosition(20, Timestamp(20)), StreamPosition(40, Timestamp(40)))
    )

    val merged = Seq(
      StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(2, Timestamp(2))),
      StreamRange("topic", 1, StreamPosition(20, Timestamp(20)), StreamPosition(40, Timestamp(40)))
    )

    StreamRange.merge(prev, next) should contain theSameElementsAs merged
  }

  it("should merge sequence of ranges correctly when topics and partitions are mixed") {
    val prev = Seq(
      StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(2, Timestamp(2))),
      StreamRange("topic", 1, StreamPosition(10, Timestamp(10)), StreamPosition(20, Timestamp(20))),
      StreamRange("topic", 2, StreamPosition(100, Timestamp(100)), StreamPosition(200, Timestamp(200)))
    )
    val next = Seq(
      StreamRange("topic", 0, StreamPosition(2, Timestamp(2)), StreamPosition(4, Timestamp(4))),
      StreamRange("topic", 1, StreamPosition(20, Timestamp(20)), StreamPosition(40, Timestamp(40))),
      StreamRange("topic", 3, StreamPosition(2000, Timestamp(2000)), StreamPosition(4000, Timestamp(4000)))
    )

    val merged = Seq(
      StreamRange("topic", 0, StreamPosition(1, Timestamp(1)), StreamPosition(4, Timestamp(4))),
      StreamRange("topic", 1, StreamPosition(10, Timestamp(10)), StreamPosition(40, Timestamp(40))),
      StreamRange("topic", 2, StreamPosition(100, Timestamp(100)), StreamPosition(200, Timestamp(200))),
      StreamRange("topic", 3, StreamPosition(2000, Timestamp(2000)), StreamPosition(4000, Timestamp(4000)))
    )

    StreamRange.merge(prev, next) should contain theSameElementsAs merged
  }
}
