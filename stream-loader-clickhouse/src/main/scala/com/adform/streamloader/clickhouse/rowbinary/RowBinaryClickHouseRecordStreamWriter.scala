/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import com.adform.streamloader.sink.batch.RecordStreamWriter

import java.io.OutputStream

/**
  * Stream writer implementation that encodes records using an implicit [[RowBinaryClickHouseRecordEncoder]]
  * and writes them to the provided output stream.
  */
class RowBinaryClickHouseRecordStreamWriter[R: RowBinaryClickHouseRecordEncoder](os: OutputStream)
    extends RecordStreamWriter[R] {

  private val pw = new RowBinaryClickHousePrimitiveTypeWriter {
    override def writeByte(b: Int): Unit = {
      os.write(b)
    }
  }
  private val recordEncoder = implicitly[RowBinaryClickHouseRecordEncoder[R]]

  override def writeRecord(record: R): Unit = {
    recordEncoder.write(record, pw)
  }

  override def close(): Unit = os.close()
}
