/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.util.zip.GZIPOutputStream

import com.adform.streamloader.batch.RecordStreamWriter
import com.adform.streamloader.util.Logging
import com.github.luben.zstd.ZstdOutputStream
import net.jpountz.lz4.LZ4BlockOutputStream
import org.anarres.lzo.{LzoCompressor1x_999, LzopOutputStream}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.xerial.snappy.SnappyHadoopCompatibleOutputStream

/**
  * A file builder based on `FileOutputStream`.
  *
  * @param recordStreamWriterFactory Factory that produces record stream writers given output streams.
  * @param compression               Compression to use.
  *
  * @tparam R type of the records written to files being built.
  */
class StreamFileBuilder[-R](
    recordStreamWriterFactory: OutputStream => RecordStreamWriter[R],
    compression: Compression,
    bufferSizeBytes: Int
) extends BaseFileBuilder[R]
    with Logging {

  override protected def createFile(): File = {
    File.createTempFile("loader-", compression.fileExtension.map("." + _).getOrElse(""))
  }

  private val fileStream = new CountingOutputStream(new FileOutputStream(file))
  private val compressedFileStream = compression match {
    case Compression.NONE => new BufferedOutputStream(fileStream, bufferSizeBytes)
    case Compression.ZSTD => new BufferedOutputStream(new ZstdOutputStream(fileStream), bufferSizeBytes)
    case Compression.GZIP => new GZIPOutputStream(fileStream, bufferSizeBytes)
    case Compression.BZIP => new BZip2CompressorOutputStream(fileStream)
    case Compression.LZOP => new LzopOutputStream(fileStream, new LzoCompressor1x_999(9), bufferSizeBytes)
    case Compression.SNAPPY => new SnappyHadoopCompatibleOutputStream(fileStream, bufferSizeBytes)
    case Compression.LZ4 => new LZ4BlockOutputStream(fileStream, bufferSizeBytes)
  }

  private val streamWriter = recordStreamWriterFactory(compressedFileStream)
  streamWriter.writeHeader()

  override def write(record: R): Unit = {
    streamWriter.writeRecord(record)
    super.write(record)
  }

  override def getDataSize: Long = fileStream.size

  override def build(): Option[File] = {
    if (!isClosed) {
      streamWriter.writeFooter()
      streamWriter.close()
    }
    super.build()
  }

  override def discard(): Unit = {
    if (!isClosed) streamWriter.close()
    super.discard()
  }
}
