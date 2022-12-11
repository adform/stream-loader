/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink

import com.adform.streamloader.model.{StreamPosition, StreamRecord}
import com.adform.streamloader.source.KafkaContext
import org.apache.kafka.common.TopicPartition

/**
  * Base trait that describes Kafka consumer record sinking to some underlying storage.
  * Implementers need to define committed position lookup for a list of assigned partitions which are then
  * used to rewind the streams. They are also responsible for implementing offset committing.
  *
  * Implementers can chose to use Kafka itself for offset storage by using the provided [[KafkaContext]],
  * they can also store/retrieve offsets to/from the storage itself. The delivery guarantees of a sink depend
  * on the offset management, e.g. storing offsets atomically with data guarantees exactly-once storage, whereas
  * using only Kafka for offset storage will usually result in either at-least-once of at-most-once semantics.
  */
trait Sink {

  /**
    * Initializes the sink with a [[KafkaContext]].
    *
    * @param kafkaContext The context provided by the source, can be used for offset management in Kafka.
    */
  def initialize(kafkaContext: KafkaContext): Unit

  /**
    * Assigns a set of partitions to the sink and returns their committed stream positions.
    * Called during the initial partitions subscription and on subsequent rebalance events.
    *
    * @param partitions Set of partitions assigned.
    * @return Committed stream position map for all the assigned partitions.
    */
  def assignPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]]

  /**
    * Revokes a set of partitions from the sink, called during rebalance events.
    * Returns committed stream positions for partitions that need rewinding.
    *
    * @param partitions Set of partitions revoked.
    * @return Committed stream position map for partitions that need rewinding.
    */
  def revokePartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]]

  /**
    * Writes a record to the underlying storage.
    *
    * @param record Stream record to write.
    */
  def write(record: StreamRecord): Unit

  /**
    * Notifies the sink that record consumption is still active, called when no records get polled from Kafka.
    * Gives the sink an opportunity to perform flushing when working with very low traffic streams.
    */
  def heartbeat(): Unit

  /**
    * Closes the sink and performs any necessary clean up.
    */
  def close(): Unit
}

object Sink {

  /**
    * Base builder trait for all sink builders.
    */
  trait Builder {
    def build(): Sink
  }
}

/**
  * A [[Sink]] that wraps another sink instance and by default forwards all operations to it.
  * Used for implementing concrete wrapping sinks in order to avoid boilerplate.
  *
  * @param baseSink The base sink to wrap.
  * @tparam S Type of sink being wrapped.
  */
abstract class WrappedSink[S <: Sink](baseSink: S) extends Sink {
  override def initialize(kafkaContext: KafkaContext): Unit = baseSink.initialize(kafkaContext)

  override def assignPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] =
    baseSink.assignPartitions(partitions)
  override def revokePartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] =
    baseSink.revokePartitions(partitions)

  override def write(record: StreamRecord): Unit = baseSink.write(record)
  override def heartbeat(): Unit = baseSink.heartbeat()

  override def close(): Unit = baseSink.close()
}
