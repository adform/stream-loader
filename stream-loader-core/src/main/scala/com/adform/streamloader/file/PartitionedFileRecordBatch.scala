/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import com.adform.streamloader.model.{RecordBatch, RecordRange}

/**
  * A record batch that is partitioned by some value, e.g. by date.
  *
  * @param partitionBatches Mapping of file record batch per partition.
  * @tparam P Type of the partitioning information, e.g. date or a tuple of client/country, etc.
  * @tparam B Type of the file record batches.
  */
case class PartitionedFileRecordBatch[P, +B <: BaseFileRecordBatch](partitionBatches: Map[P, B]) extends RecordBatch {

  def fileBatches: Seq[B] = partitionBatches.values.toSeq

  final override def recordRanges: Seq[RecordRange] = {
    fileBatches
      .flatMap(_.recordRanges)
      .groupBy(r => (r.topic, r.partition))
      .map {
        case ((topic, partition), ranges) =>
          RecordRange(topic, partition, ranges.map(_.start).min, ranges.map(_.end).max)
      }
      .toSeq
  }
}
