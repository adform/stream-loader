/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import com.adform.streamloader.file.{Compression, FileBuilder}
import com.google.protobuf.Message
import org.apache.hadoop.fs.Path
import org.apache.parquet.proto.ProtoParquetWriter

import scala.reflect.ClassTag

/**
  * Parquet file builder factory for encoding protobuf messages.
  */
class ProtoParquetFileBuilderFactory[-R <: Message: ClassTag](compression: Compression, blockSize: Int, pageSize: Int)(
    implicit currentTimeMills: () => Long = () => System.currentTimeMillis()
) extends BaseParquetFileBuilderFactory[R](compression) {

  private val recordClass = implicitly[ClassTag[R]].runtimeClass.asInstanceOf[Class[_ <: Message]]

  override def newFileBuilder(filenamePrefix: String): FileBuilder[R] = {
    val file = getFile(filenamePrefix)
    new ParquetFileBuilder[R](
      file,
      new ProtoParquetWriter[R](
        new Path(file.getAbsolutePath),
        recordClass,
        compressionCodecName,
        blockSize,
        pageSize))(currentTimeMills)
  }
}
