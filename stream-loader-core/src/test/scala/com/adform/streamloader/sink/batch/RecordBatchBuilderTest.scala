/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch

import com.adform.streamloader.model.Generators._
import com.adform.streamloader.model.{StreamPosition, StreamRange, StreamRecord, Timestamp}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RecordBatchBuilderTest extends AnyFunSpec with Matchers {

  case class EmptyRecordBatch(recordRanges: Seq[StreamRange]) extends RecordBatch {
    override def discard(): Boolean = true
  }

  object EmptyRecordBatch {
    class Builder extends RecordBatchBuilder[EmptyRecordBatch] {
      override protected def addToBatch(record: StreamRecord): Int = 1
      override def isBatchReady: Boolean = true
      override def build(): Option[EmptyRecordBatch] = Some(EmptyRecordBatch(currentRecordRanges))
      override def discard(): Unit = {}
    }
  }

  it("should correctly extend record ranges") {
    val builder = new EmptyRecordBatch.Builder()

    builder.add(newStreamRecord("topic", 1, 0, Timestamp(0L)))
    builder.add(newStreamRecord("topic", 1, 10, Timestamp(10L)))
    builder.add(newStreamRecord("topic", 1, 11, Timestamp(9L)))

    builder.add(newStreamRecord("topic", 2, 1, Timestamp(1L)))
    builder.add(newStreamRecord("topic", 2, 2, Timestamp(11L)))

    val batch = builder.build().get

    batch.recordRanges should contain theSameElementsAs Seq(
      StreamRange("topic", 1, StreamPosition(0, Timestamp(0L)), StreamPosition(11, Timestamp(10L))),
      StreamRange("topic", 2, StreamPosition(1, Timestamp(1L)), StreamPosition(2, Timestamp(11L)))
    )
  }
}
