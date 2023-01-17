/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop

import com.adform.streamloader.model.{StreamPosition, StreamRange, Timestamp}
import com.adform.streamloader.sink.batch.storage.TwoPhaseCommitMetadata
import com.adform.streamloader.sink.file.{FilePathFormatter, PartitionedFileRecordBatch, SingleFileRecordBatch}
import com.adform.streamloader.source.MockKafkaContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._

class HadoopFileStorageTest extends AnyFunSpec with Matchers {

  it("should store files to a local Hadoop file system and commit offsets") {

    val tmpDir = System.getProperty("java.io.tmpdir")
    val baseDir = new File(s"/$tmpDir/${UUID.randomUUID()}")
    baseDir.mkdirs()

    val fs = new RawLocalFileSystem()
    fs.initialize(new URI("file:///"), new Configuration())

    val formatter = new FilePathFormatter[Unit] {
      override def formatPath(partition: Unit, ranges: Seq[StreamRange]): String = "filename"
    }

    val context = new MockKafkaContext()
    val storage = HadoopFileStorage
      .builder[Unit]()
      .hadoopFS(fs)
      .stagingBasePath(s"$baseDir/staged")
      .destinationBasePath(s"$baseDir/stored")
      .destinationFilePathFormatter(formatter)
      .build()

    val tp = new TopicPartition("topic", 0)
    val (start, end) = (StreamPosition(0, Timestamp(0)), StreamPosition(10, Timestamp(100)))

    storage.initialize(context)
    storage.recover(Set(tp))

    val sourceFile = File.createTempFile("test", "txt")
    val fileBatch = SingleFileRecordBatch(sourceFile, Seq(StreamRange(tp.topic(), tp.partition(), start, end)))
    val batch = PartitionedFileRecordBatch[Unit, SingleFileRecordBatch](Map(() -> fileBatch))
    val destFile = new File(s"${baseDir.getAbsolutePath}/stored/filename")

    try {
      storage.commitBatch(batch)

      destFile.exists() shouldBe true
      context.committed(Set(tp)) shouldEqual Map(
        tp -> Some(new OffsetAndMetadata(11, TwoPhaseCommitMetadata[MultiFileStaging](Timestamp(100), None).serialize))
      )
    } finally {
      fs.close()
      sourceFile.delete()
      Files
        .walk(baseDir.toPath)
        .iterator()
        .asScala
        .toArray
        .reverse
        .foreach(_.toFile.delete())
    }
  }
}
