/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import com.adform.streamloader.file.RecordRangeFile
import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.Logging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * An abstract file storage that stores files and commits offsets to Kafka in a two phase transaction.
  * Committed stream positions are looked up in Kafka.
  * The file commit algorithm proceeds in phases as follows:
  *
  *   1. stage the file to storage,
  *   1. commit offsets to Kafka retaining the current offset and saving the new offset and
  *      file staging information to the commit metadata,
  *   1. store the staged file,
  *   1. commit new offsets to Kafka and clear the staging information from the offset metadata.
  *
  * If committing fails in the first two stages the recovery will revert it, if it fails afterwards, recovery will
  * complete the transaction.
  *
  * Implementers need to define the file staging and storing.
  *
  * @tparam F Type of file IDs.
  */
abstract class TwoPhaseCommitFileStorage[F] extends FileStorage[F] with Logging {

  /**
    * Stages a file to storage.
    *
    * @param file File to store.
    * @return Information about the staging.
    */
  protected def stageFile(file: RecordRangeFile[F]): FileStaging

  /**
    * Finalizes storage of a staged file.
    *
    * @param fileStaging File staging information.
    */
  protected def storeFile(fileStaging: FileStaging): Unit

  /**
    * Checks whether a staged file is actually stored, used during recovery.
    *
    * @param fileStaging File staging information.
    * @return Whether the file is fully stored.
    */
  protected def isFileStored(fileStaging: FileStaging): Boolean

  final override def recover(topicPartitions: Set[TopicPartition]): Unit = {
    val staged = committedOffsets(topicPartitions).collect {
      case (tp, Some((_, Some(stagedOffsetCommit)))) => (tp, stagedOffsetCommit)
    }
    val recovered = staged.map(kv => (kv._1, recoverPartition(kv._1, kv._2)))
    kafkaContext.commitSync(recovered)
  }

  private def recoverPartition(tp: TopicPartition, stagedOffsetCommit: StagedOffsetCommit): OffsetAndMetadata = {
    log.info(s"Recovering partition $tp with staged commit in Kafka $stagedOffsetCommit")
    val staging = stagedOffsetCommit.fileStaging
    if (!isFileStored(staging)) {
      log.info(
        s"File ${staging.stagingPath} staged, but not found in storage (${staging.destinationPath}), trying to store it")
      storeFile(staging)
    }
    log.info(s"Recovery for $tp succeeded, committing offsets to ${stagedOffsetCommit.end.offset + 1}")
    new OffsetAndMetadata(
      stagedOffsetCommit.end.offset + 1,
      TwoPhaseCommitMetadata(stagedOffsetCommit.end.watermark, None).toJson
    )
  }

  /**
    * Commits a given file to storage.
    */
  final override def commitFile(file: RecordRangeFile[F]): Unit = {
    val fileStaging = stageFile(file)
    stageKafkaCommit(file, fileStaging)
    storeFile(fileStaging)
    finalizeKafkaCommit(file, fileStaging)
  }

  /**
    * Gets the latest committed stream positions for the given partitions.
    */
  final override def committedPositions(
      topicPartitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]] = {
    committedOffsets(topicPartitions).map(kv => (kv._1, kv._2.map(_._1)))
  }

  private def committedOffsets(topicPartitions: Set[TopicPartition])
    : Map[TopicPartition, Option[(StreamPosition, Option[StagedOffsetCommit])]] = {
    val kafkaOffsets = kafkaContext.committed(topicPartitions)
    topicPartitions.map(tp => (tp, kafkaOffsets.get(tp).flatten.map(parseOffsetAndMetadata))).toMap
  }

  private def parseOffsetAndMetadata(om: OffsetAndMetadata): (StreamPosition, Option[StagedOffsetCommit]) = {
    val metadata = Option(om.metadata()).flatMap(TwoPhaseCommitMetadata.tryParseJson)
    (
      StreamPosition(om.offset(), metadata.map(_.watermark).getOrElse(Timestamp(0L))),
      metadata.flatMap(_.stagedOffsetCommit))
  }

  private def stageKafkaCommit(file: RecordRangeFile[F], fileStaging: FileStaging): Unit = {
    val offsets = file.recordRanges.map(
      recordRange =>
        (
          new TopicPartition(recordRange.topic, recordRange.partition),
          new OffsetAndMetadata(
            recordRange.start.offset,
            TwoPhaseCommitMetadata(
              recordRange.start.watermark,
              Some(StagedOffsetCommit(fileStaging, recordRange.start, recordRange.end))
            ).toJson
          )
      )
    )
    kafkaContext.commitSync(offsets.toMap)
  }

  private def finalizeKafkaCommit(file: RecordRangeFile[F], fileStaging: FileStaging): Unit = {
    val offsets = file.recordRanges.map(
      recordRange =>
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
