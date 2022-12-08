/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

/**
  * Specification of a range of records consumed from a single topic partition,
  * the start and end positions are both inclusive.
  */
case class StreamRange(topic: String, partition: Int, start: StreamPosition, end: StreamPosition)

/**
  * A mutable builder of [[StreamRange]].
  *
  * @param topic Topic the range is in.
  * @param partition Partition the range is in.
  * @param start The start position of the range.
  */
class StreamRangeBuilder(topic: String, partition: Int, start: StreamPosition) {
  private var StreamPosition(currentOffset, currentWatermark) = start

  /**
    * Extends the current range to the specified position.
    */
  def extend(position: StreamPosition): Unit = {
    currentOffset = position.offset
    currentWatermark = Timestamp(math.max(currentWatermark.millis, position.watermark.millis))
  }

  /**
    * Builds and returns the range.
    */
  def build(): StreamRange = StreamRange(
    topic,
    partition,
    start,
    StreamPosition(currentOffset, currentWatermark)
  )
}
