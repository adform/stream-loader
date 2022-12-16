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

import java.util.concurrent.atomic.AtomicBoolean
import com.adform.streamloader.util.{KeyCache, Logging, MetricTag, Metrics}
import com.adform.streamloader.util.{KeyCache, Logging, MetricTag, Metrics}
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener

import java.util.concurrent.atomic.AtomicBoolean

/**
  * The main stream loading class, given a [[KafkaSource]] and some [[Sink]] continuously
  * polls the source and sinks records to the sink.
  *
  * Runs in the active thread and blocks it. When running multiple instances in multiple threads
  * you must provide separate sources and sinks.
  */
trait StreamLoader {
  def start(): Unit
  def stop(): Unit
  def setMetricRegistry(registry: MeterRegistry): Unit
}

class DefaultStreamLoader private (
    source: KafkaSource,
    sink: Sink,
    consumerRebalanceListener: ConsumerRebalanceListener
) extends Logging
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
      source.subscribe(consumerRebalanceListener)

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

object DefaultStreamLoader {
  def apply(source: KafkaSource, sink: Sink): DefaultStreamLoader =
    new DefaultStreamLoader(source, sink, new DefaultConsumerRebalanceListener(source, sink))
}

class DeduplicatingStreamLoader private (
    source: KafkaSource,
    sink: Sink,
    consumerRebalanceListener: ConsumerRebalanceListener,
    cache: KeyCache[Array[Byte]]
) extends Logging
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

      val duplicates = createCounter("duplicates", Seq(MetricTag("loader-thread", Thread.currentThread().getName)))

      log.debug("Initializing source and sink")
      val kafkaContext = source.initialize()
      sink.initialize(kafkaContext)

      log.debug("Subscribing to the source")
      source.subscribe(consumerRebalanceListener)

      log.info("Starting stream loading")
      while (running.get()) {
        var recordsPolled = 0

        for (record <- source.poll()) {
          val consumerRecord = record.consumerRecord
          val key = consumerRecord.key()
          val partition = consumerRecord.partition()
          if (cache.verifyAndSwitchIfReady(partition, consumerRecord.offset()) && !cache.contains(partition, key)) {
            sink.write(record)
          } else {
            duplicates.increment()
          }
          cache.add(partition, key)
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

object DeduplicatingStreamLoader {
  def apply(source: KafkaSource, sink: Sink, cache: KeyCache[Array[Byte]]): DeduplicatingStreamLoader =
    new DeduplicatingStreamLoader(source, sink, new DeduplicationConsumerRebalanceListener(source, sink, cache), cache)
}

object StreamLoader {
  def withDeduplication(source: KafkaSource, sink: Sink, cache: KeyCache[Array[Byte]]): DeduplicatingStreamLoader =
    DeduplicatingStreamLoader(source, sink, cache)

  def default(source: KafkaSource, sink: Sink): DefaultStreamLoader =
    DefaultStreamLoader(source, sink)
}
