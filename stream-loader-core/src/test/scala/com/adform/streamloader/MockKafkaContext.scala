/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

class MockKafkaContext extends KafkaContext {
  val commits = mutable.Map.empty[TopicPartition, Seq[OffsetAndMetadata]]

  private var commitsInvoked = 0
  private var failOnCommit = -1

  def failOnCommitOnce(seqNo: Int = 1): Unit = {
    commitsInvoked = 0
    failOnCommit = seqNo
  }

  override val consumerGroup: String = "mock-consumer-group"

  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    commitsInvoked += 1
    if (failOnCommit == commitsInvoked) {
      throw new UnsupportedOperationException("Offset commit failed")
    }
    offsets.foreach(o => commits.updateWith(o._1)(offs => Some(offs.getOrElse(Seq.empty) :+ o._2)))
  }

  override def committed(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[OffsetAndMetadata]] =
    topicPartitions.map(tp => (tp, commits.get(tp).map(_.last))).toMap
}
