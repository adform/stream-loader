/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.io.File

import com.adform.streamloader.model.{RecordRange, RecordRangeBuilder, StreamPosition}
import org.apache.kafka.common.TopicPartition

import scala.collection.concurrent.TrieMap

/**
  * A tuple of a file, it's ID and the ranges of records it contains.
  */
case class RecordRangeFile[F](file: File, fileId: F, recordRanges: Seq[RecordRange], recordCount: Long)

/**
  * A [[RecordRangeFile]] builder.
  */
class RecordRangeFileBuilder[-R, F](val fileBuilder: FileBuilder[R], val fileId: F) {
  private val recordRangeBuilders: TrieMap[TopicPartition, RecordRangeBuilder] = TrieMap.empty

  /**
    * Writes a record to the file.
    */
  def writeRecord(record: R): Unit = fileBuilder.write(record)

  /**
    * Extends the ranges contained within the file.
    */
  def extendRange(tp: TopicPartition, position: StreamPosition): Unit = {
    val recordRangeBuilder =
      recordRangeBuilders.getOrElseUpdate(tp, new RecordRangeBuilder(tp.topic(), tp.partition(), position))

    recordRangeBuilder.extend(position)
  }

  /**
    * Finalizes and builds the file.
    */
  def build(): Option[RecordRangeFile[F]] =
    fileBuilder
      .build()
      .map(f => RecordRangeFile(f, fileId, recordRangeBuilders.values.map(_.build()).toSeq, fileBuilder.getRecordCount))
}
