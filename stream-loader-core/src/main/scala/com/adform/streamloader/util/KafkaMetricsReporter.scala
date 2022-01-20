/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.util.function.ToDoubleFunction

import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}
import org.apache.kafka.common.{Metric, MetricName}

import scala.jdk.CollectionConverters._

/**
  * An implementation of the Kafka metric reporter that registers all Kafka metrics
  * to a global metric registry.
  */
class KafkaMetricsReporter extends MetricsReporter with Metrics {

  private class MetricEval extends ToDoubleFunction[Metric] {
    def applyAsDouble(m: Metric): Double = {
      m.metricValue() match {
        case v: java.lang.Double => v
        case _ => 0.0
      }
    }
  }

  private val metricEval = new MetricEval

  val metricsRoot: String = ""

  private def metricName(metric: Metric): String = {
    // Prometheus does not allow registering metrics with the same name and different tag sets,
    // which does happen with Kafka, e.g. it measures "records-lag-max" per per topic, but also per partition,
    // so we make metric names unique by appending all tag keys to the name except "client-id".
    val tagKeys = metric.metricName().tags().asScala.keys.toList.filter(_ != "client-id").sorted.mkString(".")
    val tagPostfix = if (tagKeys.nonEmpty) s".by.$tagKeys" else ""
    val group = metric.metricName().group()
    val name = metric.metricName().name()
    s"kafka.$group.$name$tagPostfix"
  }

  private def metricTags(metric: Metric): Seq[MetricTag] =
    metric.metricName().tags().asScala.map(t => MetricTag(t._1, t._2)).toSeq

  private def registerKafkaMetricGauge(metric: KafkaMetric): Unit = {
    // There is only one metric without the "client-id" tag that counts the total number of metrics, ignore it
    if (metric.metricName().tags().containsKey("client-id")) {
      createGauge[Metric](
        name = metricName(metric),
        metric = metric,
        tdf = metricEval,
        tags = metricTags(metric)
      )
    }
  }

  override def init(metrics: java.util.List[KafkaMetric]): Unit =
    metrics.asScala.foreach(registerKafkaMetricGauge)

  override def configure(configs: java.util.Map[String, _]): Unit = {}

  override def metricChange(metric: KafkaMetric): Unit = registerKafkaMetricGauge(metric)

  override def metricRemoval(metric: KafkaMetric): Unit = {
    // Since micrometer does not allow removing metrics, we re-register it to expose a constant value of 0.0 instead
    if (metric.metricName().tags().containsKey("client-id")) {
      createGauge[Metric](
        name = metricName(metric),
        metric = zeroMetric(metric.metricName()),
        tdf = metricEval,
        tags = metricTags(metric)
      )
    }
  }

  def zeroMetric(mn: MetricName): Metric = new Metric {
    override def metricName(): MetricName = mn
    override def metricValue(): Object = java.lang.Double.valueOf(0)
  }

  override def close(): Unit = {}
}
