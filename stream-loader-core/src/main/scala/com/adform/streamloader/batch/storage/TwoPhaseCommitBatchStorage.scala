/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch.storage

import com.adform.streamloader.batch.RecordBatch
import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.{JsonSerializer, Logging}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.json4s.native.JsonMethods.{compact, render}

/**
  * An abstract batch storage that stores batches and commits offsets to Kafka in a two phase transaction.
  * Committed stream positions are looked up in Kafka.
  * The batch commit algorithm proceeds in phases as follows:
  *
  *   1. stage the batch to storage (e.g. upload file to a temporary path),
  *   1. stage offsets to Kafka by performing an offset commit without modifying the actual offset, instead
  *      saving the new offset and the staged batch information (e.g. file path of the temporary uploaded file)
  *      serialized as JSON to the offset commit metadata field,
  *   1. store the staged batch (e.g. move the temporary file to the final destination)
  *   1. commit new offsets to Kafka and clear the staging information from the offset metadata.
  *
  * If committing fails in the first two stages the recovery will revert it, if it fails afterwards, recovery will
  * complete the transaction.
  *
  * Implementers need to define the batch staging and storing.
  *
  * @tparam B Type of record batches.
  * @tparam S Type of the batch staging information, must be JSON serializable.
  */
abstract class TwoPhaseCommitBatchStorage[-B <: RecordBatch, S: JsonSerializer]
    extends RecordBatchStorage[B]
    with Logging {

  /**
    * Stages a record batch to storage.
    *
    * @param batch Record batch to store.
    * @return Information about the staging.
    */
  protected def stageBatch(batch: B): S

  /**
    * Finalizes storage of a staged record batch.
    *
    * @param staging Batch staging information.
    */
  protected def storeBatch(staging: S): Unit

  /**
    * Checks whether a staged batch is actually stored, used during recovery.
    *
    * @param staging Batch staging information.
    * @return Whether the batch is fully stored.
    */
  protected def isBatchStored(staging: S): Boolean

  final override def recover(topicPartitions: Set[TopicPartition]): Unit = {
    val staged = committedOffsets(topicPartitions).collect { case (tp, Some((_, Some(stagedOffsetCommit)))) =>
      (tp, stagedOffsetCommit)
    }
    val recovered = staged.map(kv => (kv._1, recoverPartition(kv._1, kv._2)))
    kafkaContext.commitSync(recovered)
  }

  private def recoverPartition(tp: TopicPartition, stagedOffsetCommit: StagedOffsetCommit[S]): OffsetAndMetadata = {
    log.info(s"Recovering partition $tp with staged commit in Kafka $stagedOffsetCommit")
    val staging = stagedOffsetCommit.staging
    if (!isBatchStored(staging)) {
      val stagingJson = compact(render(implicitly[JsonSerializer[S]].serialize(staging)))
      log.info(s"Record batch staging found, storing it: $stagingJson")
      storeBatch(staging)
    }
    log.info(s"Recovery for $tp succeeded, committing offsets to ${stagedOffsetCommit.end.offset + 1}")
    new OffsetAndMetadata(
      stagedOffsetCommit.end.offset + 1,
      TwoPhaseCommitMetadata(stagedOffsetCommit.end.watermark, None).toJson
    )
  }

  /**
    * Commits a given batch to storage.
    */
  final override def commitBatch(batch: B): Unit = {
    val staging = stageBatch(batch)
    stageKafkaCommit(batch, staging)
    storeBatch(staging)
    finalizeKafkaCommit(batch, staging)
  }

  /**
    * Gets the latest committed stream positions for the given partitions.
    */
  final override def committedPositions(
      topicPartitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]] = {
    committedOffsets(topicPartitions).map(kv => (kv._1, kv._2.map(_._1)))
  }

  private def committedOffsets(
      topicPartitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[(StreamPosition, Option[StagedOffsetCommit[S]])]] = {
    val kafkaOffsets = kafkaContext.committed(topicPartitions)
    topicPartitions.map(tp => (tp, kafkaOffsets.get(tp).flatten.map(parseOffsetAndMetadata))).toMap
  }

  private def parseOffsetAndMetadata(om: OffsetAndMetadata): (StreamPosition, Option[StagedOffsetCommit[S]]) = {
    val metadata = Option(om.metadata()).flatMap(TwoPhaseCommitMetadata.tryParseJson[S])
    (
      StreamPosition(om.offset(), metadata.map(_.watermark).getOrElse(Timestamp(0L))),
      metadata.flatMap(_.stagedOffsetCommit)
    )
  }

  private def stageKafkaCommit(batch: B, staging: S): Unit = {
    val offsets = batch.recordRanges.map(recordRange =>
      (
        new TopicPartition(recordRange.topic, recordRange.partition),
        new OffsetAndMetadata(
          recordRange.start.offset,
          TwoPhaseCommitMetadata(
            recordRange.start.watermark,
            Some(StagedOffsetCommit(staging, recordRange.start, recordRange.end))
          ).toJson
        )
      )
    )
    kafkaContext.commitSync(offsets.toMap)
  }

  private def finalizeKafkaCommit(batch: B, staging: S): Unit = {
    val offsets = batch.recordRanges.map(recordRange =>
      (
        new TopicPartition(recordRange.topic, recordRange.partition),
        new OffsetAndMetadata(
          recordRange.end.offset + 1,
          TwoPhaseCommitMetadata(recordRange.end.watermark, None).toJson
        )
      )
    )
    kafkaContext.commitSync(offsets.toMap)
  }
}
