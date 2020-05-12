/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

/**
  * Represents a position in a single partition of a stream.
  *
  * @param offset The numeric offset with the partition, i.e. the Kafka offset.
  * @param watermark The watermark, i.e. maximum timestamp seen up to that point.
  */
case class StreamPosition(offset: Long, watermark: Timestamp) extends Ordered[StreamPosition] {
  override def compare(that: StreamPosition): Int = offset.compare(that.offset)
}
