/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.model.{StreamRecord, StreamPosition}
import com.adform.streamloader.util.Logging
import org.apache.kafka.common.TopicPartition

import scala.collection.concurrent.TrieMap

/**
  * An abstract sink that implements partition assignment/revocation by grouping partitions to sets
  * using a user defined function and delegates the actual data sinking to instances of [[PartitionGroupSinker]],
  * which operate with fixed sets of partitions and thus do not need to worry about rebalance events.
  *
  * Implementers must define a partition grouping, which is a mapping from a topic partition to a group name string,
  * the simplest grouping would simply map all partitions to a `"root"` group, a more elaborate grouping could create
  * separate groups for smaller and larger topics, each group receiving a separate [[PartitionGroupSinker]].
  * E.g., if the sinker writes all data to a file, you would end up with separate files for smaller and larger topics.
  */
abstract class PartitionGroupingSink extends Sink with Logging {

  private val partitionGroups = TrieMap[String, (Set[TopicPartition], PartitionGroupSinker)]()
  private val partitionSinkers = TrieMap[TopicPartition, PartitionGroupSinker]()

  protected var kafkaContext: KafkaContext = _

  override def initialize(context: KafkaContext): Unit = {
    kafkaContext = context
  }

  /**
    * Maps a given topic partition to a group name.
    *
    * @return A group name, e.g. `"root"` or `topicPartition.topic()`.
    */
  def groupForPartition(topicPartition: TopicPartition): String

  /**
    * Creates a new instance of a [[PartitionGroupSinker]] for a given partition group.
    * These instances are closed and re-created during rebalance events.
    *
    * @param groupName Name of the partition group.
    * @param partitions Partitions in the group.
    * @return A new instance of a sinker for the given group.
    */
  def sinkerForPartitionGroup(groupName: String, partitions: Set[TopicPartition]): PartitionGroupSinker

  final override def assignPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    log.info(s"Assigned partitions ${partitions.mkString(", ")}, grouping them")

    val groupPositions = partitions.groupBy(groupForPartition).map { case (group, groupPartitions) =>
      // Check if we already have a sinker for this group, if so, close it and collect the previously owned partitions
      val oldGroupPartitions = partitionGroups
        .get(group)
        .map { case (tps, currentGroupSinker) =>
          log.info(s"Closing existing partition sinker for ${tps.mkString(", ")}")
          currentGroupSinker.close()
          partitionGroups.remove(group)
          tps.foreach(partitionSinkers.remove)
          tps
        }
        .getOrElse(Set.empty)

      log.info(
        s"Creating and initializing a new sinker for partition group '$group' containing " +
          s"newly assigned partitions ${groupPartitions
              .mkString(", ")} and previously owned partitions ${oldGroupPartitions.mkString(", ")}"
      )

      val newGroupPartitions = groupPartitions ++ oldGroupPartitions
      val sinker = sinkerForPartitionGroup(group, newGroupPartitions)
      val positions = sinker.initialize(kafkaContext)

      partitionGroups.put(group, newGroupPartitions -> sinker)
      newGroupPartitions.foreach(tp => partitionSinkers.put(tp, sinker))
      positions
    }

    groupPositions.flatMap(_.toList).toMap
  }

  final override def revokePartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    if (partitions.nonEmpty) {
      log.info(s"Revoked partitions ${partitions.mkString(", ")}")

      val groupPositions = partitions.groupBy(groupForPartition).map { case (group, _) =>
        val remainingGroupPartitions = partitionGroups
          .get(group)
          .map { case (currentGroupPartitions, currentGroupSinker) =>
            log.info(s"Closing existing group '$group' partition sinker for ${currentGroupPartitions.mkString(", ")}")
            currentGroupSinker.close()
            partitionGroups.remove(group)
            currentGroupPartitions.foreach(partitionSinkers.remove)
            currentGroupPartitions -- partitions
          }
          .getOrElse(Set.empty)

        // The group still contains partitions, re-create the sinker with the remaining set
        if (remainingGroupPartitions.nonEmpty) {
          log.info(
            s"Creating and initializing a new sinker for partition group '$group' containing " +
              s"remaining partitions ${remainingGroupPartitions.mkString(", ")}"
          )

          val sinker = sinkerForPartitionGroup(group, remainingGroupPartitions)
          val positions = sinker.initialize(kafkaContext)

          partitionGroups.put(group, remainingGroupPartitions -> sinker)
          remainingGroupPartitions.foreach(tp => partitionSinkers.put(tp, sinker))

          positions
        } else {
          Map.empty
        }
      }

      groupPositions.flatMap(_.toList).toMap
    } else {
      Map.empty
    }
  }

  /**
    * Forwards a consumer record to the correct partition sinker.
    *
    * @param record Stream record to write.
    */
  override def write(record: StreamRecord): Unit = {
    partitionSinkers(record.topicPartition).write(record)
  }

  /**
    * Forwards the heartbeat to all sinkers.
    */
  override def heartbeat(): Unit = {
    partitionSinkers.foreach { case (_, sinker) => sinker.heartbeat() }
  }

  /**
    * Closes all active sinkers.
    */
  override def close(): Unit = {
    partitionGroups.foreach { case (group, (groupPartitions, sinker)) =>
      log.info(s"Closing sinker for group '$group' containing partitions ${groupPartitions.mkString(",")}")
      sinker.close()
    }
  }
}
