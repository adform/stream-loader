/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import java.util.Optional

object Generators {

  def newStreamRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Timestamp,
      key: String,
      value: String
  ): StreamRecord = {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      partition,
      offset,
      timestamp.millis,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      key.getBytes("UTF-8"),
      value.getBytes("UTF-8"),
      new RecordHeaders,
      Optional.empty[Integer]
    )
    StreamRecord(cr, timestamp)
  }

  def newStreamRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Timestamp
  ): StreamRecord = {
    newStreamRecord(topic, partition, offset, timestamp, "", "")
  }
}
