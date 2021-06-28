/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.s3

import java.io.File
import java.util.UUID

import com.adform.streamloader.MockKafkaContext
import com.adform.streamloader.file.{FilePathFormatter, FileRecordBatch}
import com.adform.streamloader.model.{RecordRange, StreamPosition, Timestamp}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, ListObjectsRequest}

import scala.jdk.CollectionConverters._

class S3FileStorageTest extends AnyFunSpec with Matchers with MockS3 {

  it("should store files to a mock S3 file system and commit offsets") {

    val formatter = new FilePathFormatter {
      override def formatPath(ranges: Seq[RecordRange]): String = "filename"
    }

    val bucket = UUID.randomUUID().toString

    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())

    val context = new MockKafkaContext()
    val storage = S3FileStorage
      .builder()
      .s3Client(s3Client)
      .bucket(bucket)
      .filePathFormatter(formatter)
      .build()

    val tp = new TopicPartition("topic", 0)
    val (start, end) = (StreamPosition(0, Timestamp(0)), StreamPosition(10, Timestamp(100)))

    storage.initialize(context)
    storage.recover(Set(tp))

    val sourceFile = File.createTempFile("test", "txt")
    val batch = FileRecordBatch(sourceFile, Seq(RecordRange(tp.topic(), tp.partition(), start, end)))

    try {
      storage.commitBatch(batch)
      val stored = s3Client
        .listObjects(ListObjectsRequest.builder().bucket(bucket).prefix("").build())
        .contents()
        .asScala
        .map(_.key())
        .toArray

      stored should contain theSameElementsAs Array("filename")
      context.committed(Set(tp)) shouldEqual Map(tp -> Some(new OffsetAndMetadata(11, "{\"watermark\":100}")))

    } finally {
      sourceFile.delete()
    }
  }
}
