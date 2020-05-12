/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.model.StreamPosition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable

class PartitionGroupingSinkTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class TestTopicGroupingSink extends PartitionGroupingSink {
    val partitionSinkers: mutable.HashSet[PartitionGroupSinker] = mutable.HashSet.empty[PartitionGroupSinker]

    override def groupForPartition(topicPartition: TopicPartition): String = topicPartition.topic()

    override def sinkerForPartitionGroup(name: String, partitions: Set[TopicPartition]): PartitionGroupSinker = {
      val sinker = new PartitionGroupSinker {
        override val groupName: String = name
        override val groupPartitions: Set[TopicPartition] = partitions
        override def initialize(kc: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = Map.empty
        override def write(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {}
        override def heartbeat(): Unit = {}
        override def close(): Unit = partitionSinkers.remove(this)
      }
      partitionSinkers.add(sinker)
      sinker
    }
  }

  it("should continue loading a partition group when some of its partitions get revoked") {

    val sink = new TestTopicGroupingSink

    sink.assignPartitions(
      Set(
        new TopicPartition("test", 0),
        new TopicPartition("test", 1),
        new TopicPartition("test", 2)
      )
    )

    sink.revokePartitions(
      Set(
        new TopicPartition("test", 0)
      )
    )

    sink.partitionSinkers.size shouldBe 1

    sink.partitionSinkers.flatMap(_.groupPartitions) should contain theSameElementsAs Set(
      new TopicPartition("test", 1),
      new TopicPartition("test", 2)
    )
  }

  it("should stop loading a partition group when all its partitions get revoked") {

    val sink = new TestTopicGroupingSink

    sink.assignPartitions(
      Set(
        new TopicPartition("test", 0),
        new TopicPartition("test", 1)
      )
    )

    sink.revokePartitions(
      Set(
        new TopicPartition("test", 0),
        new TopicPartition("test", 1)
      )
    )

    sink.partitionSinkers.size shouldBe 0
  }

  it("should not create additional groups when partitions get assigned to existing groups") {

    val sink = new TestTopicGroupingSink

    sink.assignPartitions(
      Set(
        new TopicPartition("test", 0),
        new TopicPartition("test", 1)
      )
    )

    sink.assignPartitions(
      Set(
        new TopicPartition("test", 2)
      )
    )

    sink.partitionSinkers.size shouldBe 1

    sink.partitionSinkers.flatMap(_.groupPartitions) should contain theSameElementsAs Set(
      new TopicPartition("test", 0),
      new TopicPartition("test", 1),
      new TopicPartition("test", 2)
    )
  }

  it("should create new loaders when new partition groups get assigned") {

    val sink = new TestTopicGroupingSink

    sink.assignPartitions(
      Set(
        new TopicPartition("test", 0),
        new TopicPartition("test", 1)
      )
    )

    sink.assignPartitions(
      Set(
        new TopicPartition("new", 0)
      )
    )

    sink.partitionSinkers.size shouldBe 2

    sink.partitionSinkers.flatMap(_.groupPartitions) should contain theSameElementsAs Set(
      new TopicPartition("test", 0),
      new TopicPartition("test", 1),
      new TopicPartition("new", 0)
    )
  }
}
