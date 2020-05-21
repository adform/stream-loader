/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.adform.streamloader.file.storage.FileStorage
import com.adform.streamloader.model._
import com.adform.streamloader.util.Retry._
import com.adform.streamloader.util._
import com.adform.streamloader.{KafkaContext, PartitionGroupSinker}
import io.micrometer.core.instrument.{Counter, Gauge, Meter, Timer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.concurrent.TrieMap

/**
  * A [[PartitionGroupSinker]] that loads records to some file based storage.
  *
  * @param groupName The partition group name (used for metrics and logs).
  * @param groupPartitions Set of Kafka topic partitions to sink.
  * @param fileRecordFormatter Mapping from source records to records of type `R` stored in files.
  * @param fileBuilderFactory File builder for the record type `R`.
  * @param fileStorage The file storage to use.
  * @param validWatermarkDiffMillis Upper limit for setting watermarks greater than currentTimeMillis.
  * @param retryPolicy The retry policy to use for all retriable operations.
  *
  * @tparam R Type of records persisted to files.
  * @tparam F Type of the file IDs stored.
  */
class FilePartitionGroupSinker[R, F](
    override val groupName: String,
    override val groupPartitions: Set[TopicPartition],
    fileRecordFormatter: FileRecordFormatter[R, F],
    fileBuilderFactory: FileBuilderFactory[R],
    fileStorage: FileStorage[F],
    fileCommitStrategy: FileCommitStrategy,
    fileCommitQueueSize: Int,
    validWatermarkDiffMillis: Long,
    retryPolicy: Retry.Policy
)(implicit currentTimeMills: () => Long = () => System.currentTimeMillis())
    extends PartitionGroupSinker
    with Logging
    with Metrics { self =>

  override val metricsRoot = "stream_loader.file"

  protected var kafkaContext: KafkaContext = _

  private var isInitialized = false
  private val isRunning = new AtomicBoolean(false)

  private var builder: RecordRangeFileBuilder[R, F] = _
  private val watermarks: TrieMap[TopicPartition, Timestamp] = TrieMap.empty

  private val commitQueue = new ArrayBlockingQueue[RecordRangeFile[F]](fileCommitQueueSize)
  private val commitThread = new Thread(
    () => {
      while (isRunning.get()) try {

        def fileCommittedAfterFailure(rrf: RecordRangeFile[F]): Boolean = retryOnFailure(retryPolicy) {
          fileStorage.recover(groupPartitions)
          fileStorage.isFileCommitted(rrf)
        }

        val rrf = commitQueue.take()

        log.info(s"Committing file ${rrf.file.getAbsolutePath} to storage")
        Metrics.commitDuration.recordCallable(() =>
          retryOnFailureIf(retryPolicy)(!fileCommittedAfterFailure(rrf)) {
            fileStorage.commitFile(rrf)
        })
        if (rrf.file.exists() && !rrf.file.delete()) {
          log.warn(s"Failed deleting file ${rrf.file.getAbsolutePath}")
        }
      } catch {
        case e if isInterruptionException(e) =>
          log.debug("File commit thread interrupted")
      }
    },
    s"${Thread.currentThread().getName}-$groupName-file-commit-thread" // e.g. loader-1-root-file-commit-thread
  )

  override def initialize(context: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
    if (isInitialized)
      throw new IllegalStateException(s"Loader for '$groupName' already initialized")

    kafkaContext = context

    log.info(s"Recovering storage for partitions ${groupPartitions.mkString(", ")}")

    retryOnFailure(retryPolicy) {
      fileStorage.recover(groupPartitions)
    }

    log.info(s"Looking up offsets for partitions ${groupPartitions.mkString(", ")}")
    val positions = fileStorage.committedPositions(groupPartitions)
    positions.foreach {
      case (tp, position) => watermarks.put(tp, position.map(_.watermark).getOrElse(Timestamp(-1L)))
    }

    startNewFile()

    isRunning.set(true)
    commitThread.start()

    isInitialized = true

    positions
  }

  override def write(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = if (isRunning.get()) {
    if (!isInitialized)
      throw new IllegalStateException("Loader has to be initialized before starting writes")

    // Update watermark
    val tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
    val recordTimestamp = Timestamp(consumerRecord.timestamp())
    val watermark = if (recordTimestamp.millis <= currentTimeMills() + validWatermarkDiffMillis) {
      val currentWatermark = watermarks(tp)
      if (recordTimestamp.millis > currentWatermark.millis) {
        watermarks(tp) = recordTimestamp
        recordTimestamp
      } else {
        currentWatermark
      }
    } else {
      log.warn(
        s"Received a message with an out of bounds timestamp $recordTimestamp (" +
          recordTimestamp.format("yyyy/MM/dd HH:mm:ss").get +
          s") at $tp offset ${consumerRecord.offset()}.")
      watermarks(tp)
    }

    // Write records to file
    fileRecordFormatter
      .format(Record(consumerRecord, watermark), builder.fileId)
      .foreach { record =>
        builder.writeRecord(record)
        Metrics.recordsWritten(tp).increment()
      }

    // Extend ranges contained in file
    builder.extendRange(tp, StreamPosition(consumerRecord.offset(), watermark))

    // Check if file needs to be committed
    checkAndCommitFileIfNeeded()
  }

  override def heartbeat(): Unit = checkAndCommitFileIfNeeded()

  private def checkAndCommitFileIfNeeded(): Unit = {
    if (fileCommitStrategy.shouldCommit(
          builder.fileBuilder.getOpenDuration,
          builder.fileBuilder.getDataSize,
          builder.fileBuilder.getRecordCount)) {

      log.info(
        s"Closing file for '$groupName' and putting it to the commit queue " +
          s"after it being open for ${builder.fileBuilder.getOpenDuration.toMillis} ms " +
          s"with ${builder.fileBuilder.getRecordCount} records and ${builder.fileBuilder.getDataSize} bytes written"
      )

      builder.build().foreach { file =>
        try {
          log.debug(s"File ${file.file.getAbsolutePath} closed, queuing it for commit to storage")
          commitQueue.put(file)
        } catch {
          case e if isInterruptionException(e) =>
            log.debug("Loader interrupted while putting file to commit queue")
            return
        }
      }

      startNewFile()
    }
  }

  private def startNewFile(): Unit = retryOnFailure(retryPolicy) {
    builder = new RecordRangeFileBuilder[R, F](
      fileBuilderFactory.newFileBuilder(s"$groupName-"),
      fileStorage.startNewFile()
    )
  }

  override def close(): Unit = if (isRunning.compareAndSet(true, false)) {
    log.info(s"Closing partition loader for '$groupName', discarding the current file")
    builder.build().foreach(_.file.delete())

    log.debug("Interrupting file commit thread and waiting for it to stop")
    commitThread.interrupt()
    commitThread.join()

    log.debug("Closing and removing meters")
    Metrics.allMeters.foreach(meter => {
      meter.close()
      removeMeters(meter)
    })
  }

  object Metrics {

    private val commonTags = Seq(
      MetricTag("partition-group", groupName),
      MetricTag("loader-thread", Thread.currentThread().getName)
    )

    private def partitionTags(tp: TopicPartition) =
      Seq(MetricTag("topic", tp.topic()), MetricTag("partition", tp.partition().toString))

    val partitionWatermarkDelays: Set[Gauge] = groupPartitions.map(
      tp =>
        createGauge(
          "watermark.delay.ms",
          self,
          (_: FilePartitionGroupSinker[R, F]) =>
            watermarks.get(tp).map(w => currentTimeMills() - w.millis).getOrElse(0L).toDouble,
          commonTags ++ partitionTags(tp)
      ))

    val recordsWritten: Map[TopicPartition, Counter] =
      groupPartitions.map(tp => tp -> createCounter("records.written", commonTags ++ partitionTags(tp))).toMap

    val commitDuration: Timer = createTimer("commit.duration", commonTags, maxDuration = Duration.ofMinutes(5))
    val commitQueueSize: Gauge =
      createGauge("commit.queue.size", self, (_: FilePartitionGroupSinker[R, F]) => self.commitQueue.size(), commonTags)

    val allMeters: Seq[Meter] =
      Seq(commitDuration, commitQueueSize) ++ recordsWritten.values ++ partitionWatermarkDelays
  }
}
