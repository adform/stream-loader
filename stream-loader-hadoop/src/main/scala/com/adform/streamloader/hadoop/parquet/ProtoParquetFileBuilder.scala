/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import com.adform.streamloader.file.BaseFileBuilder
import com.google.protobuf.Message
import org.apache.hadoop.fs.Path
import org.apache.parquet.proto.ProtoParquetWriter

import java.io.File
import java.util.UUID
import scala.reflect.ClassTag

/**
  * A parquet file writer that writes protobuf encoded messages.
  */
class ProtoParquetFileBuilder[R <: Message: ClassTag](config: ParquetConfig) extends BaseFileBuilder[R] {

  override protected def createFile(): File = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    val filename = UUID.randomUUID().toString
    new File(s"$tmpDir/$filename.parquet")
  }

  private val parquetWriter = {
    val builder = ProtoParquetWriter.builder[R](new Path(file.getAbsolutePath))
    builder.withMessage(implicitly[ClassTag[R]].runtimeClass.asInstanceOf[Class[_ <: Message]])
    config.applyTo(builder)
    builder.build()
  }

  override def write(record: R): Unit = {
    parquetWriter.write(record)
    super.write(record)
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
