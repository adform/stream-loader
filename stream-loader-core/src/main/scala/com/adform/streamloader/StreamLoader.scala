/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.sink.Sink
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.{Logging, MetricTag, Metrics}
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._

/**
  * The main stream loading class, given a $KafkaSource and some $Sink continuously
  * polls the source and sinks records to the sink.
  *
  * Runs in the active thread and blocks it. When running multiple instances in multiple threads
  * you must provide separate sources and sinks.
  *
  * @define KafkaSource [[com.adform.streamloader.source.KafkaSource KafkaSource]]
  * @define Sink [[com.adform.streamloader.sink.Sink Sink]]
  */
class StreamLoader(source: KafkaSource, sink: Sink) extends Logging with Metrics {

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
            sink.revokePartitions(partitions).foreach {
              case (tp, Some(position)) =>
                log.info(s"Resetting offset for $tp to $position")
                source.seek(tp, position)
              case (tp, None) =>
                log.info(s"No committed offset found for $tp, resetting to default offset")
            }
          }

          override def onPartitionsAssigned(tps: java.util.Collection[TopicPartition]): Unit = {
            val partitions = tps.asScala.toSet
            log.info(s"Assigning partitions to stream loader: ${partitions.mkString(", ")}")
            sink.assignPartitions(partitions).foreach {
              case (tp, Some(position)) =>
                log.info(s"Resetting offset for $tp to $position")
                source.seek(tp, position)
              case (tp, None) =>
                log.info(s"No committed offset found for $tp, resetting to default offset")
            }
          }
        }
      )

      log.info("Starting stream loading")
      while (running.get()) {
        var recordsPolled = 0

        for (record <- source.poll()) {
          sink.write(record)
          recordsPolled += 1
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
