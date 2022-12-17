/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import com.adform.streamloader.model.StreamRecord

object TestExtensions {

  implicit class RichStreamRecord(val record: StreamRecord) extends AnyVal {

    /**
      * Gets the topic-partition to offset mapping for a given record.
      */
    def tpo: (String, Long) = record.topicPartition.toString -> record.consumerRecord.offset()
  }
}
