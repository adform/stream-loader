/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import org.apache.kafka.common.TopicPartition

/**
  * Specification of a range of records consumed from a single topic partition,
  * the start and end positions are both inclusive.
  */
case class StreamRange(topic: String, partition: Int, start: StreamPosition, end: StreamPosition) {
  def topicPartition: TopicPartition = new TopicPartition(topic, partition)
}

object StreamRange {

  /**
    * Merges two consecutive ranges from the same topic partition to a new combined range.
    */
  def merge(previous: StreamRange, next: StreamRange): StreamRange = {
    assert(
      previous.topic == next.topic && previous.partition == next.partition,
      "Only ranges from the same topic partition can be merged"
    )

    StreamRange(previous.topic, previous.partition, previous.start, next.end)
  }

  /**
    * Merges consecutive ranges by topic partition into a new combined set of ranges.
    */
  def merge(previous: Seq[StreamRange], next: Seq[StreamRange]): Seq[StreamRange] = {
    val tps = Set(previous.map(r => (r.topic, r.partition)) ++ next.map(r => (r.topic, r.partition)): _*)

    tps.map { case (topic, partition) =>
      val maybePrevious = previous.find(r => r.topic == topic && r.partition == partition)
      val maybeNext = next.find(r => r.topic == topic && r.partition == partition)
      StreamRange(
        topic,
        partition,
        maybePrevious.getOrElse(maybeNext.get).start,
        maybeNext.getOrElse(maybePrevious.get).end
      )
    }.toSeq
  }
}

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
