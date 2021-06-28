/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch

import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.adform.streamloader.batch.storage.RecordBatchStorage
import com.adform.streamloader.model._
import com.adform.streamloader.util.Retry._
import com.adform.streamloader.util._
import com.adform.streamloader.{KafkaContext, PartitionGroupSinker}
import io.micrometer.core.instrument.{Counter, Gauge, Meter, Timer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.concurrent.TrieMap

/**
  * A [[PartitionGroupSinker]] that accumulates records to batches and stores them to some storage once ready.
  *
  * @param groupName The partition group name (used for metrics and logs).
  * @param groupPartitions Set of Kafka topic partitions to sink.
  * @param recordBatcher A record batcher to use.
  * @param batchStorage A storage to use.
  * @param batchCommitQueueSize Size of the batch commit queue, once full further attempts to store batches will block.
  * @param validWatermarkDiffMillis Upper limit for setting watermarks greater than currentTimeMillis.
  * @param retryPolicy The retry policy to use for all retriable operations.
  * @tparam B Type of record batches persisted to storage.
  */
class RecordBatchingSinker[B <: RecordBatch](
    override val groupName: String,
    override val groupPartitions: Set[TopicPartition],
    recordBatcher: RecordBatcher[B],
    batchStorage: RecordBatchStorage[B],
    batchCommitQueueSize: Int,
    validWatermarkDiffMillis: Long,
    retryPolicy: Retry.Policy
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends PartitionGroupSinker
    with Logging
    with Metrics {
  self =>

  override val metricsRoot = "stream_loader.batch"

  protected var kafkaContext: KafkaContext = _

  private var isInitialized = false
  private val isRunning = new AtomicBoolean(false)

  private var builder: RecordBatchBuilder[B] = _
  private val watermarks: TrieMap[TopicPartition, Timestamp] = TrieMap.empty

  private val commitQueue = new ArrayBlockingQueue[B](batchCommitQueueSize)
  private val commitThread = new Thread(
    () => {
      while (isRunning.get()) try {

        def batchCommittedAfterFailure(batch: B): Boolean = retryOnFailure(retryPolicy) {
          batchStorage.recover(groupPartitions)
          batchStorage.isBatchCommitted(batch)
        }

        val batch = commitQueue.take()

        log.info(s"Committing batch $batch to storage")
        Metrics.commitDuration.recordCallable(() =>
          retryOnFailureIf(retryPolicy)(!batchCommittedAfterFailure(batch)) {
            batchStorage.commitBatch(batch)
        })
      } catch {
        case e if isInterruptionException(e) =>
          log.debug("Batch commit thread interrupted")
      }
    },
    s"${Thread.currentThread().getName}-$groupName-batch-commit-thread" // e.g. loader-1-root-batch-commit-thread
  )

  override def initialize(context: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
    if (isInitialized)
      throw new IllegalStateException(s"Loader for '$groupName' already initialized")

    kafkaContext = context

    log.info(s"Recovering storage for partitions ${groupPartitions.mkString(", ")}")

    retryOnFailure(retryPolicy) {
      batchStorage.recover(groupPartitions)
    }

    log.info(s"Looking up offsets for partitions ${groupPartitions.mkString(", ")}")
    val positions = batchStorage.committedPositions(groupPartitions)
    positions.foreach {
      case (tp, position) => watermarks.put(tp, position.map(_.watermark).getOrElse(Timestamp(-1L)))
    }

    startNewBatch()

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
    val watermark = if (recordTimestamp.millis <= timeProvider.currentMillis + validWatermarkDiffMillis) {
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

    // Write records to batch
    builder.add(Record(consumerRecord, watermark))
    Metrics.recordsWritten(tp).increment()

    // Check if batch needs to be committed
    checkAndCommitBatchIfNeeded()
  }

  override def heartbeat(): Unit = checkAndCommitBatchIfNeeded()

  private def checkAndCommitBatchIfNeeded(): Unit = {
    if (builder.isBatchReady) {
      log.info(s"Forming batch for '$groupName' and putting it to the commit queue")
      builder.build().foreach { batch =>
        try {
          log.debug(s"Batch $batch formed, queuing it for commit to storage")
          commitQueue.put(batch)
        } catch {
          case e if isInterruptionException(e) =>
            log.debug("Loader interrupted while putting batch to commit queue")
            return
        }
      }

      startNewBatch()
    }
  }

  private def startNewBatch(): Unit = retryOnFailure(retryPolicy) {
    builder = recordBatcher.newBatchBuilder()
  }

  override def close(): Unit = if (isRunning.compareAndSet(true, false)) {
    log.info(s"Closing partition loader for '$groupName', discarding the current batch")
    builder.discard()

    log.debug("Interrupting batch commit thread and waiting for it to stop")
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
          (_: RecordBatchingSinker[B]) =>
            watermarks.get(tp).map(w => timeProvider.currentMillis - w.millis).getOrElse(0L).toDouble,
          commonTags ++ partitionTags(tp)
      ))

    val recordsWritten: Map[TopicPartition, Counter] =
      groupPartitions.map(tp => tp -> createCounter("records.written", commonTags ++ partitionTags(tp))).toMap

    val commitDuration: Timer = createTimer("commit.duration", commonTags, maxDuration = Duration.ofMinutes(5))
    val commitQueueSize: Gauge =
      createGauge("commit.queue.size", self, (_: RecordBatchingSinker[B]) => self.commitQueue.size(), commonTags)

    val allMeters: Seq[Meter] =
      Seq(commitDuration, commitQueueSize) ++ recordsWritten.values ++ partitionWatermarkDelays
  }
}
