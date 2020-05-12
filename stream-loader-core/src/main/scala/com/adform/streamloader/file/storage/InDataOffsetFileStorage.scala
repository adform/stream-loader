/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import com.adform.streamloader.file.RecordRangeFile
import com.adform.streamloader.util.Logging
import com.adform.streamloader.util.Retry.isInterruptionException
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.util.control.NonFatal

/**
  * File storage that commits offsets atomically together with data and also stores them to Kafka on a best effort basis.
  * On lookup offsets are retrieved from the storage, the offsets in Kafka are not used.
  * No recovery is needed in this case as file storing is assumed to be atomic.
  */
abstract class InDataOffsetFileStorage[F] extends FileStorage[F] with Logging {

  override def recover(topicPartitions: Set[TopicPartition]): Unit = {}

  /**
    * Stores a given file to storage together with the offsets.
    */
  def storeFile(file: RecordRangeFile[F]): Unit

  final override def commitFile(file: RecordRangeFile[F]): Unit = {
    storeFile(file)
    try {
      log.info(
        s"Committing offsets to Kafka: ${file.recordRanges.map(r => s"${r.topic}-${r.partition}: ${r.end.offset + 1}").mkString(", ")}")
      kafkaContext.commitSync(
        file.recordRanges
          .map(
            r =>
              new TopicPartition(r.topic, r.partition) ->
                new OffsetAndMetadata(r.end.offset + 1, s"""{ "watermark": ${r.end.watermark.millis} }"""))
          .toMap
      )
      log.debug("Committed offsets to Kafka successfully")
    } catch {
      case NonFatal(e) if !isInterruptionException(e) =>
        log.warn(e)("Failed committing offsets to Kafka, ignoring and continuing")
    }
  }
}
