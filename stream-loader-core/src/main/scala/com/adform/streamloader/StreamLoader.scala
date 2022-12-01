/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import java.util.concurrent.atomic.AtomicBoolean
import com.adform.streamloader.util.{FifoHashSet, KeyCache, Logging, MetricTag, Metrics}
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

/**
  * The main stream loading class, given a [[KafkaSource]] and some [[Sink]] continuously
  * polls the source and sinks records to the sink.
  *
  * Runs in the active thread and blocks it. When running multiple instances in multiple threads
  * you must provide separate sources and sinks.
  */
class StreamLoader(source: KafkaSource, sink: Sink, cache: KeyCache[Array[Byte]] = KeyCache.noop())
    extends Logging
    with Metrics {

  override protected def metricsRoot: String = "stream_loader"

  private val running: AtomicBoolean = new AtomicBoolean(false)

  /**
    * Starts stream loading in the current thread, blocks until `stop()` is called from another thread.
    */
  def start(): Unit =
    if (running.compareAndSet(false, true)) {

      val recordsPolledDistribution =
        createDistribution("kafka.records.polled", Seq(MetricTag("loader-thread", Thread.currentThread().getName)))

      log.debug("Initializing source and sink")
      val kafkaContext = source.initialize()
      sink.initialize(kafkaContext)

      log.debug("Subscribing to the source")
      source.subscribe(
        new ConsumerRebalanceListener {
          override def onPartitionsRevoked(tps: java.util.Collection[TopicPartition]): Unit = {
            val partitions = tps.asScala.toSet
            log.info(s"Revoking partitions from stream loader: ${partitions.mkString(", ")}")
            val revokedPartitions = sink.revokePartitions(partitions)
            revokedPartitions.foreach {
              case (tp, Some(position)) =>
                log.info(s"Resetting offset for $tp to $position")
                cache.revokePartitions(tp.partition())
                source.seek(tp, position)
              case (tp, None) =>
                cache.revokePartitions(tp.partition())
                log.info(s"No committed offset found for $tp, resetting to default offset")
            }
          }

          override def onPartitionsAssigned(tps: java.util.Collection[TopicPartition]): Unit = {
            val partitions = tps.asScala.toSet
            log.info(s"Assigning partitions to stream loader: ${partitions.mkString(", ")}")
            val assignedPartitions = sink.assignPartitions(partitions)
            assignedPartitions.foreach {
              case (tp, Some(position)) =>
                val cachePosition = calculateOffset(position.offset, cache.keysSize())
                log.info(s"Resetting offset for $tp to $position, cache offset from: $cachePosition")
                cache.assignPartition(tp.partition(), position.offset, ready = false)
                source.seek(tp, cachePosition)
              case (tp, None) =>
                cache.assignPartition(tp.partition(), 0, ready = true)
                log.info(s"No committed offset found for $tp, resetting to default offset")
            }
          }
        }
      )

      log.info("Starting stream loading")
      while (running.get()) {
        var recordsPolled = 0

        for (record <- source.poll()) {
          val key = record.key()
          val partition = record.partition()
          if (cache.switchIfReady(partition, record.offset()) && !cache.contains(partition, key)) {
            sink.write(record)
            recordsPolled += 1
          }
          cache.add(partition, key)
        }

        if (recordsPolled == 0)
          sink.heartbeat()

        log.trace(s"Polled $recordsPolled records")
        recordsPolledDistribution.record(recordsPolled.toDouble)
      }

      log.info("Stopping stream loader")
      sink.close()
      source.close()
      recordsPolledDistribution.close()
    }

  private def calculateOffset(offset: Long, cacheSize: Long): Long = Math.max(0, offset - cacheSize)

  /**
    * Stops the stream loader, performs any necessary clean up.
    */
  def stop(): Unit = running.compareAndSet(true, false)

  /**
    * Sets the `MeterRegistry` to register all loader metrics to.
    */
  def setMetricRegistry(registry: MeterRegistry): Unit = {
    Metrics.registry = registry
  }
}
