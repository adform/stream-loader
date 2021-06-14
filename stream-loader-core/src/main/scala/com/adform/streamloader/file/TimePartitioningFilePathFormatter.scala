/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import com.adform.streamloader.model.RecordRange
import com.adform.streamloader.util.TimeExtractor

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.hashing.MurmurHash3

/**
  * Formats file paths placing them into time-based directories constructed by formatting the partition using
  * a given time formatter pattern, e.g. "/dt=2020-06-01/" etc.
  * The filename itself is a UUID based on the hash of the ranges contained, for reproducibility.
  *
  * @param timePartitionPattern Pattern for the time partition, e.g. 'dt='yyyyMMdd
  * @param fileExtension The file extension to use
  */
class TimePartitioningFilePathFormatter[P: TimeExtractor](
    timePartitionPattern: Option[String],
    fileExtension: Option[String]
) extends FilePathFormatter[P] {

  override def formatPath(partition: P, ranges: Seq[RecordRange]): String = {
    val fileExtensionStr = fileExtension.map(ext => s".$ext").getOrElse("")

    val time = implicitly[TimeExtractor[P]].extractTime(partition)
    val timePartition = timePartitionPattern
      .map(pattern => time.format(pattern).get + "/")
      .getOrElse("")

    def expandHash(hash: Int, times: Int): Seq[Int] = {
      Seq.fill(times - 1)(hash).scan(hash)((h1, h2) => h1 * 31 + h2)
    }
    val hashBytes = ByteBuffer.allocate(16)
    expandHash(MurmurHash3.seqHash(ranges), times = 4).foreach(hashBytes.putInt)

    val fileName = new UUID(hashBytes.getLong(0), hashBytes.getLong(1))

    s"$timePartition$fileName$fileExtensionStr"
  }
}
