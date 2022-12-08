/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.model.{StreamRecord, StreamPosition, Timestamp}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable

class PartitionGroupingSinkTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class TestTopicGroupingSink extends PartitionGroupingSink {
    val partitionSinkers: mutable.HashSet[PartitionGroupSinker] = mutable.HashSet.empty[PartitionGroupSinker]
    val groupValuesWritten: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer.empty[(String, String)]

    override def groupForPartition(topicPartition: TopicPartition): String = topicPartition.topic()

    override def sinkerForPartitionGroup(name: String, partitions: Set[TopicPartition]): PartitionGroupSinker = {
      val sinker = new PartitionGroupSinker {
        override val groupName: String = name
        override val groupPartitions: Set[TopicPartition] = partitions
        override def initialize(kc: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = Map.empty
        override def write(record: StreamRecord): Unit = {
          groupValuesWritten.addOne(name -> new String(record.consumerRecord.value(), "UTF-8"))
        }
        override def heartbeat(): Unit = {}
        override def close(): Unit = partitionSinkers.remove(this)
      }
      partitionSinkers.add(sinker)
      sinker
    }

    def writeValue(topic: String, partition: Int, value: String): Unit = {
      write(
        StreamRecord(
          new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, 0, Array.empty[Byte], value.getBytes("UTF-8")),
          Timestamp(0L)
        )
      )
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

  it("should stop sinking a partition group when all its partitions get revoked") {

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

  it("should create new sinkers when new partition groups get assigned") {

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

  it("should continue sinking records after new partitions get assigned to existing group") {
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

    sink.writeValue("test", 0, "a")
    sink.writeValue("test", 1, "b")
    sink.writeValue("test", 2, "c")

    sink.groupValuesWritten should contain theSameElementsAs Seq(
      "test" -> "a",
      "test" -> "b",
      "test" -> "c"
    )
  }

  it("should continue sinking records after partitions get revoked from existing group") {
    val sink = new TestTopicGroupingSink

    sink.assignPartitions(
      Set(
        new TopicPartition("test", 0),
        new TopicPartition("test", 1)
      )
    )

    sink.revokePartitions(
      Set(
        new TopicPartition("test", 1)
      )
    )

    sink.writeValue("test", 0, "a")

    sink.groupValuesWritten should contain theSameElementsAs Seq("test" -> "a")
  }
}
