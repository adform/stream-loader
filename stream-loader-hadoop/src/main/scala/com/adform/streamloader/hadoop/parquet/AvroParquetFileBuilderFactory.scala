/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import java.io.File
import java.time.Duration

import com.adform.streamloader.file.{Compression, FileBuilder}
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

/**
  * Parquet file builder for records of any data type that have implicitly defined Avro encoders.
  */
class AvroParquetFileBuilderFactory[R: Encoder: Decoder: SchemaFor](compression: Compression)(
    implicit currentTimeMills: () => Long = () => System.currentTimeMillis()
) extends BaseParquetFileBuilderFactory[R](compression) {

  private val recordFormat = RecordFormat[R]

  override def newFileBuilder(filenamePrefix: String): FileBuilder[R] = {
    val file = getFile(filenamePrefix)
    val writer = AvroParquetWriter
      .builder[GenericRecord](new Path(file.getAbsolutePath))
      .withSchema(AvroSchema[R])
      .withConf(new Configuration())
      .withCompressionCodec(compressionCodecName)
      .build()
    val genericBuilder = new ParquetFileBuilder(file, writer)(currentTimeMills)

    new FileBuilder[R] {
      override def write(record: R): Unit = genericBuilder.write(recordFormat.to(record))
      override def getDataSize: Long = genericBuilder.getDataSize
      override def getRecordCount: Long = genericBuilder.getRecordCount
      override def getOpenDuration: Duration = genericBuilder.getOpenDuration
      override def build(): Option[File] = genericBuilder.build()
    }
  }
}
