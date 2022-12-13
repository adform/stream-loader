/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.sink.Sink
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.{KeyCache, Logging}
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import java.util
import scala.jdk.CollectionConverters._

class DefaultConsumerRebalanceListener(source: KafkaSource, sink: Sink) extends ConsumerRebalanceListener with Logging {

  override def onPartitionsRevoked(tps: util.Collection[TopicPartition]): Unit = {
    val partitions = tps.asScala.toSet
    log.info(s"Revoking partitions from stream loader: ${partitions.mkString(", ")}")
    val revokedPartitions = sink.revokePartitions(partitions)
    revokedPartitions.foreach {
      case (tp, Some(position)) =>
        log.info(s"Resetting offset for $tp to $position")
        source.seek(tp, position)
      case (tp, None) =>
        log.info(s"No committed offset found for $tp, resetting to default offset")
    }
  }

  override def onPartitionsAssigned(tps: util.Collection[TopicPartition]): Unit = {
    val partitions = tps.asScala.toSet
    log.info(s"Assigning partitions to stream loader: ${partitions.mkString(", ")}")
    val assignedPartitions = sink.assignPartitions(partitions)
    assignedPartitions.foreach {
      case (tp, Some(position)) =>
        log.info(s"Resetting offset for $tp to $position.offset")
        source.seek(tp, position)
      case (tp, None) =>
        log.info(s"No committed offset found for $tp, resetting to default offset")
    }
  }

  private def calculateOffset(offset: Long, cacheSize: Long): Long = Math.max(0, offset - cacheSize)
}
