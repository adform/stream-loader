/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.time.Duration
import java.util.function.ToDoubleFunction

import io.micrometer.core.instrument._
import io.micrometer.core.instrument.composite.CompositeMeterRegistry

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap

case class MetricTag(name: String, value: String) {
  def toSeq: Seq[String] = Seq(name, value)
}

/**
  * Convenience trait for importing metric creation methods. All exposed metrics are registered to a global registry.
  */
trait Metrics {

  /**
    * A common prefix for all created metrics.
    */
  protected def metricsRoot: String

  private def joinPrefixes(ps: Iterable[String]): String = ps.filter(_ != "").mkString(".")

  protected def createTimer(name: String, tags: Seq[MetricTag] = Seq(), maxDuration: Duration = null): Timer =
    Timer
      .builder(joinPrefixes(Seq(metricsRoot, name)))
      .maximumExpectedValue(maxDuration)
      .tags(tags.flatMap(_.toSeq): _*)
      .publishPercentileHistogram()
      .register(Metrics.registry)

  protected def createCounter(name: String, tags: Seq[MetricTag] = Seq()): Counter =
    Counter
      .builder(joinPrefixes(Seq(metricsRoot, name)))
      .tags(tags.flatMap(_.toSeq): _*)
      .register(Metrics.registry)

  protected def createGauge[T <: AnyRef](
      name: String,
      metric: T,
      tdf: ToDoubleFunction[T],
      tags: Seq[MetricTag] = Seq()
  ): Gauge = {
    val uniqueMetricName = s"$name:${tags.toList.sortBy(_.name).map(t => s"${t.name}=${t.value}").mkString(":")}"
    Metrics.gauges.put(uniqueMetricName, metric)
    Gauge
      .builder(
        joinPrefixes(Seq(metricsRoot, name)),
        Metrics.gauges,
        (gauges: concurrent.Map[String, Any]) => tdf.applyAsDouble(gauges.get(uniqueMetricName).head.asInstanceOf[T])
      )
      .tags(tags.flatMap(_.toSeq): _*)
      .register(Metrics.registry)
  }

  protected def createDistribution(name: String, tags: Seq[MetricTag] = Seq()): DistributionSummary =
    DistributionSummary
      .builder(joinPrefixes(Seq(metricsRoot, name)))
      .tags(tags.flatMap(_.toSeq): _*)
      .publishPercentileHistogram()
      .register(Metrics.registry)

  protected def removeMeters(meters: Meter*): Unit = meters.foreach(Metrics.registry.remove)
}

object Metrics {

  // Micrometer currently does not allow re-registering metrics, which is a problem with gauges,
  // since the object reporting the metric might be abandoned and re-created (e.g. a Kafka consumer).
  // As a workaround we always register all gauges to look up the object in this map, which can get updated.
  private val gauges = TrieMap[String, Any]()

  // Default registry, can be overridden before starting StreamLoader instances by
  // calling `setMetricRegistry` on an instance of a `StreamLoader`.
  private[streamloader] var registry: MeterRegistry = new CompositeMeterRegistry()
}
