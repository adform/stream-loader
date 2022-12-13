/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

import java.time.Duration
import com.adform.streamloader.model.{StreamRange, StreamRecord}
import com.adform.streamloader.sink.batch.{RecordBatchBuilder, RecordBatcher, RecordFormatter}
import com.adform.streamloader.util.TimeProvider

/**
  * A record batcher that passes records through a custom record formatter and forms batches by writing
  * the formatted records to files using a provided file builder.
  *
  * @param recordFormatter A formatter to use for writing records to files.
  * @param fileBuilderFactory A file builder to use.
  * @param fileCommitStrategy Strategy for completing batches.
  *
  * @tparam R Type of records being written to files.
  * @tparam B Type of record batches being built.
  * @tparam FB Type of file builder being used.
  */
abstract class FileRecordBatcher[R, +B <: FileRecordBatch, FB <: FileBuilder[R]](
    recordFormatter: RecordFormatter[R],
    fileBuilderFactory: FileBuilderFactory[R, FB],
    fileCommitStrategy: FileCommitStrategy
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends RecordBatcher[B] {

  /**
    * Constructs the final batch of a concrete type given the formed file and the accumulated record statistics.
    */
  def constructBatch(fileBuilder: FB, recordRanges: Seq[StreamRange], recordCount: Long): Option[B]

  override def newBatchBuilder(): RecordBatchBuilder[B] = {
    val fileStartTimeMillis = timeProvider.currentMillis
    val fileBuilder = fileBuilderFactory.newFileBuilder()

    new RecordBatchBuilder[B] {

      override def add(record: StreamRecord): Unit = {
        super.add(record)
        recordFormatter
          .format(record)
          .foreach(formatted => fileBuilder.write(formatted))
      }

      override def isBatchReady: Boolean = fileCommitStrategy.shouldCommit(
        Duration.ofMillis(timeProvider.currentMillis - fileStartTimeMillis),
        fileBuilder.getDataSize,
        fileBuilder.getRecordCount
      )

      override def build(): Option[B] = constructBatch(fileBuilder, currentRecordRanges, currentRecordCount)

      override def discard(): Unit = fileBuilder.discard()
    }
  }
}
