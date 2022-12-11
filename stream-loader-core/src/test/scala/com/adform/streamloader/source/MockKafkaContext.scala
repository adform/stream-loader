/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.source

import com.adform.streamloader.model.{StreamPosition, StreamRecord, Timestamp}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

class MockKafkaContext extends KafkaContext {
  private val commits = mutable.Map.empty[TopicPartition, Seq[OffsetAndMetadata]]
  private val writes = mutable.Map.empty[TopicPartition, Seq[OffsetAndTimestamp]]

  private var commitsInvoked = 0
  private var failOnCommit = -1

  def failOnCommitOnce(seqNo: Int = 1): Unit = {
    commitsInvoked = 0
    failOnCommit = seqNo
  }

  def write(topicPartition: TopicPartition, offsetAndTimestamp: OffsetAndTimestamp): Unit = {
    writes.updateWith(topicPartition)(offsets => Some(offsets.getOrElse(Seq.empty) :+ offsetAndTimestamp))
  }

  def write(topic: String, partition: Short, offset: Long, timestamp: Long): Unit = {
    write(new TopicPartition(topic, partition), new OffsetAndTimestamp(offset, timestamp))
  }

  def write(record: StreamRecord): Unit = {
    write(
      record.topicPartition,
      new OffsetAndTimestamp(record.consumerRecord.offset(), record.consumerRecord.timestamp())
    )
  }

  def commitWrites(): Map[TopicPartition, Option[StreamPosition]] = {
    val positionsToCommit = writes.map { case (tp, tpWrites) =>
      val offset = tpWrites.last.offset() + 1
      val watermark = tpWrites.map(_.timestamp()).max
      tp -> Some(StreamPosition(offset, Timestamp(watermark)))
    }.toMap
    commitSync(positionsToCommit.map(kv => (kv._1, new OffsetAndMetadata(kv._2.get.offset))))
    positionsToCommit
  }

  override val consumerGroup: String = "mock-consumer-group"

  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    commitsInvoked += 1
    if (failOnCommit == commitsInvoked) {
      throw new UnsupportedOperationException("Offset commit failed")
    }
    offsets.foreach { case (tp, om) =>
      commits.updateWith(tp)(offs => Some(offs.getOrElse(Seq.empty) :+ om))
    }
  }

  override def offsetsForTimes(ts: Map[TopicPartition, Long]): Map[TopicPartition, Option[OffsetAndTimestamp]] = {
    ts.map { case (topicPartition, timestampToFind) =>
      val watermark = writes.get(topicPartition).flatMap(_.find(o => o.timestamp() >= timestampToFind))
      topicPartition -> watermark
    }
  }

  override def committed(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[OffsetAndMetadata]] =
    topicPartitions.map(tp => (tp, commits.get(tp).map(_.last))).toMap
}
