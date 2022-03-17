/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import java.io.File
import java.util.UUID

import com.adform.streamloader.file.BaseFileBuilder
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile

/**
  * Base class for Avro based parquet file builders.
  *
  * @param schema Avro schema to use.
  * @param config Parquet file configuration.
  * @tparam R type of the records being added.
  */
abstract class AvroParquetFileBuilder[R](schema: Schema, config: ParquetConfig = ParquetConfig())
    extends BaseFileBuilder[R] {

  override protected def createFile(): File = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    val filename = UUID.randomUUID().toString
    new File(s"$tmpDir/$filename.parquet")
  }

  protected val parquetWriter: ParquetWriter[GenericRecord] = {
    val conf = new Configuration()
    val builder = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(new Path(file.getAbsolutePath), conf))
      .withSchema(schema)
      .withConf(conf)
    config.applyTo(builder)
    builder.build()
  }

  override def getDataSize: Long = parquetWriter.getDataSize

  override def build(): Option[File] = {
    if (!isClosed) parquetWriter.close()
    super.build()
  }

  override def discard(): Unit = {
    if (!isClosed) parquetWriter.close()
    super.discard()
  }
}

/**
  * Parquet writer that derives the Avro schema and encoder at compile time from the record type.
  */
class DerivedAvroParquetFileBuilder[R: Encoder: Decoder: SchemaFor](config: ParquetConfig = ParquetConfig())
    extends AvroParquetFileBuilder[R](AvroSchema[R], config) {

  private val recordFormat = RecordFormat[R]

  override def write(record: R): Unit = {
    parquetWriter.write(recordFormat.to(record))
    super.write(record)
  }
}

/**
  * A generic avro parquet writer that uses a user provider schema and accepts generic Avro records.
  */
class GenericAvroParquetFileBuilder(schema: Schema, config: ParquetConfig = ParquetConfig())
    extends AvroParquetFileBuilder[GenericRecord](schema, config) {

  override def write(record: GenericRecord): Unit = {
    parquetWriter.write(record)
    super.write(record)
  }
}
