/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import java.io.File

import com.adform.streamloader.MockKafkaContext
import com.adform.streamloader.file.RecordRangeFile
import com.adform.streamloader.model.{RecordRange, StreamPosition, Timestamp}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TwoPhaseCommitFileStorageTest extends AnyFunSpec with Matchers {

  private val exampleFile = RecordRangeFile[Unit](
    new File("/tmp/file.parquet"),
    (),
    Seq(
      RecordRange(
        "topic",
        0,
        StreamPosition(0, Timestamp(1570109555000L)),
        StreamPosition(100, Timestamp(1570109655000L))),
      RecordRange(
        "topic",
        1,
        StreamPosition(50, Timestamp(1570109565000L)),
        StreamPosition(150, Timestamp(1570109685000L)))
    ),
    150
  )

  private val secondFile = RecordRangeFile[Unit](
    new File("/tmp/file2.parquet"),
    (),
    Seq(
      RecordRange(
        "topic",
        0,
        StreamPosition(101, Timestamp(1570109655001L)),
        StreamPosition(200, Timestamp(1570109655100L))),
      RecordRange(
        "topic",
        1,
        StreamPosition(51, Timestamp(1570109685001L)),
        StreamPosition(300, Timestamp(1570109686000L)))
    ),
    150
  )

  private val filePartitions: Set[TopicPartition] = Set(new TopicPartition("topic", 0), new TopicPartition("topic", 1))

  private def fileStoredPath(file: RecordRangeFile[_]): String = file.file.getAbsolutePath
  private def fileOffsets(file: RecordRangeFile[_]): Map[TopicPartition, Some[OffsetAndMetadata]] =
    file.recordRanges
      .map(
        range =>
          (
            new TopicPartition(range.topic, range.partition),
            Some(new OffsetAndMetadata(range.end.offset + 1, TwoPhaseCommitMetadata(range.end.watermark, None).toJson))
        )
      )
      .toMap

  it("should perform two-phase committing") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(exampleFile)
  }

  it("should not modify stream positions if file staging fails") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    try {
      storage.failOnFileStagingOnce()
      storage.commitFile(secondFile)
    } catch {
      case _: UnsupportedOperationException =>
    }

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    storage.storedFiles should not contain fileStoredPath(secondFile)

    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(exampleFile)
  }

  it("should recover correctly after failed file staging") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    try {
      storage.failOnFileStagingOnce()
      storage.commitFile(secondFile)
    } catch {
      case _: UnsupportedOperationException =>
    }

    storage.recover(filePartitions)

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    storage.storedFiles should not contain fileStoredPath(secondFile)

    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(exampleFile)
  }

  it("should not modify stream positions if Kafka offset commit staging fails") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    try {
      context.failOnCommitOnce()
      storage.commitFile(secondFile)
    } catch {
      case _: UnsupportedOperationException =>
    }

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    storage.storedFiles should not contain fileStoredPath(secondFile)

    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(exampleFile)
  }

  it("should recover correctly if Kafka offset commit staging fails") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    try {
      context.failOnCommitOnce()
      storage.commitFile(secondFile)
    } catch {
      case _: UnsupportedOperationException =>
    }

    storage.recover(filePartitions)

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    storage.storedFiles should not contain fileStoredPath(secondFile)

    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(exampleFile)
  }

  it("should recover correctly if file storage fails") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    try {
      storage.failOnFileStoreOnce()
      storage.commitFile(secondFile)
    } catch {
      case _: UnsupportedOperationException =>
    }

    storage.recover(filePartitions)

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    storage.storedFiles should contain(fileStoredPath(secondFile))

    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(secondFile)
  }

  it("should recover correctly if Kafka offset commit fails during finalizing") {
    val context = new MockKafkaContext
    val storage = new MockTwoPhaseCommitFileStorage()

    storage.initialize(context)

    storage.commitFile(exampleFile)

    try {
      context.failOnCommitOnce(seqNo = 2)
      storage.commitFile(secondFile)
    } catch {
      case _: UnsupportedOperationException =>
    }

    storage.recover(filePartitions)

    storage.storedFiles should contain(fileStoredPath(exampleFile))
    storage.storedFiles should contain(fileStoredPath(secondFile))

    context.committed(filePartitions) should contain theSameElementsAs fileOffsets(secondFile)
  }
}
