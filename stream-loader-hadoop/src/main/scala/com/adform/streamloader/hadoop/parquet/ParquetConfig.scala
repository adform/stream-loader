/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop.parquet

import com.adform.streamloader.sink.file.Compression
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

case class ParquetConfig(
    compression: Option[Compression] = None,
    rowGroupSize: Option[Long] = None,
    pageSize: Option[Int] = None,
    pageRowCountLimit: Option[Int] = None,
    dictionaryPageSize: Option[Int] = None,
    writerVersion: Option[WriterVersion] = None,
    enableDictionaryEncoding: Option[Boolean] = None,
    enableBloomFilter: Option[Boolean] = None,
    enableByteStreamSplit: Option[Boolean] = None
) {

  private def toHadoopCompression(compression: Compression): CompressionCodecName = compression match {
    case Compression.NONE => CompressionCodecName.UNCOMPRESSED
    case Compression.ZSTD => CompressionCodecName.ZSTD
    case Compression.GZIP => CompressionCodecName.GZIP
    case Compression.SNAPPY => CompressionCodecName.SNAPPY
    case Compression.LZ4 => CompressionCodecName.LZ4
    case _ => throw new UnsupportedOperationException(s"Compression '$compression' is unsupported in parquet")
  }

  def applyTo[R](builder: ParquetWriter.Builder[R, _]): Unit = {
    compression.map(toHadoopCompression).foreach(c => builder.withCompressionCodec(c))
    rowGroupSize.foreach(r => builder.withRowGroupSize(r))
    pageSize.foreach(s => builder.withPageSize(s))
    pageRowCountLimit.foreach(p => builder.withPageRowCountLimit(p))
    dictionaryPageSize.foreach(s => builder.withDictionaryPageSize(s))
    writerVersion.foreach(v => builder.withWriterVersion(v))
    enableDictionaryEncoding.foreach(e => builder.withDictionaryEncoding(e))
    enableBloomFilter.map(e => builder.withBloomFilterEnabled(e))
    enableByteStreamSplit.foreach(e => builder.withByteStreamSplitEncoding(e))
  }
}
