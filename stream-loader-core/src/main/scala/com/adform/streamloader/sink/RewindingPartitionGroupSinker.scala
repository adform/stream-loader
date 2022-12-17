/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink

import com.adform.streamloader.model.{StreamInterval, StreamPosition, StreamRecord, Timestamp}
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.Logging
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import scala.collection.mutable

/**
  * A wrapper sinker that rewinds the streams back by a given interval during initialization.
  * Can be used to implement stateful sinks that need to "warm-up" before starting actual writing, e.g. in order to
  * implement record de-duplication one can rewind the streams in order to build-up a cache.
  * Implementers need to override the method for "touching" rewound records, once the sinker catches up all new records
  * are simply passed down to the base sinker.
  *
  * Rewinding by an offset range is straightforward as we simply subtract, a caveat here is that we can't determine the
  * rewound watermark, so we retain it. Rewinding by watermark is done by calling offsetForTimes in Kafka, which
  * can in principle return a result that is not exactly consistent with the watermark calculated by the stream loader.
  * Either way the rewinding should be considered to be done on a best-effort basis.
  *
  * @param baseSinker A base sinker to wrap.
  * @param rewindInterval A stream interval to rewind backwards.
  */
abstract class RewindingPartitionGroupSinker(baseSinker: PartitionGroupSinker, rewindInterval: StreamInterval)
    extends WrappedPartitionGroupSinker(baseSinker)
    with Logging {

  private type Offsets = Map[TopicPartition, Option[StreamPosition]]

  private var originalOffsets: Offsets = _

  private val topicPartitionsFullyRewound: mutable.Map[TopicPartition, Boolean] = mutable.Map.empty
  private var fullyRewound: Boolean = false

  override def initialize(kafkaContext: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
    originalOffsets = super.initialize(kafkaContext)
    if (rewindInterval.isZero) {
      log.info(s"Rewind interval is zero, skipping rewinding and using original offsets $originalOffsets")
      fullyRewound = true
      originalOffsets
    } else {
      groupPartitions.foreach(tp => topicPartitionsFullyRewound.addOne(tp -> false))
      val rewoundOffsets = rewindInterval match {
        case StreamInterval.OffsetRange(diff) => rewindByOffset(originalOffsets, diff)
        case StreamInterval.WatermarkRange(diff) => rewindByWatermark(originalOffsets, diff, kafkaContext)
      }
      log.info(s"Rewound original offsets $originalOffsets to $rewoundOffsets, warming up")
      rewoundOffsets
    }
  }

  private def rewindByOffset(offsets: Offsets, rewindBy: Long): Offsets = {
    val rewound = offsets.map { case (tp, pos) =>
      (tp, pos.map(p => p.copy(offset = Math.max(0, p.offset - rewindBy), p.watermark)))
    }

    log.debug(s"Translated offsets $offsets to $rewound by subtracting $rewindBy")
    rewound
  }

  private def rewindByWatermark(offsets: Offsets, rewindBy: Duration, kafkaContext: KafkaContext): Offsets = {
    val toLookup = offsets.collect {
      case (tp, Some(StreamPosition(_, w))) if w.millis > 0 =>
        (tp, Math.max(0, w.millis - rewindBy.toMillis))
    }
    val translated = kafkaContext.offsetsForTimes(toLookup)

    log.debug(s"Kafka translated rewound watermark positions $toLookup to offsets $translated")
    offsets.map { case (tp, _) =>
      (tp, translated.get(tp).flatMap(_.map(ot => StreamPosition(ot.offset(), Timestamp(ot.timestamp())))))
    }
  }

  /**
    * Process a given stream record that is already written by the base sink, but is now rewound back
    * during initialization for warm-up.
    *
    * @param record Stream record to "touch".
    */
  protected def touchRewoundRecord(record: StreamRecord): Unit

  /**
    * Checks whether the given record is a "rewound" record, i.e. is already committed by the base sink and if so
    * only "touches" it, otherwise passes it further down to the base sink.
    */
  override def write(record: StreamRecord): Unit = {
    if (fullyRewound || topicPartitionsFullyRewound(record.topicPartition)) {
      super.write(record)
    } else {
      val isRewoundRecord = originalOffsets(record.topicPartition) match {
        case Some(StreamPosition(committedOffset, _)) => record.consumerRecord.offset() < committedOffset
        case _ => false
      }
      if (isRewoundRecord) {
        touchRewoundRecord(record)
      } else {
        topicPartitionsFullyRewound(record.topicPartition) = true
        log.info(s"Partition ${record.topicPartition} fully rewound")

        if (topicPartitionsFullyRewound.forall { case (_, isRewound) => isRewound }) {
          log.info("All partitions fully rewound")
          fullyRewound = true
        }

        super.write(record)
      }
    }
  }
}
