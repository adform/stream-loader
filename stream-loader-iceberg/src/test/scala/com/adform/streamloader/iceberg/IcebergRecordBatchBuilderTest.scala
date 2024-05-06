/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.iceberg

import com.adform.streamloader.model.Generators._
import com.adform.streamloader.model.{StreamRecord, Timestamp}
import com.adform.streamloader.sink.file.FileCommitStrategy.ReachedAnyOf
import com.adform.streamloader.sink.file.MultiFileCommitStrategy
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.{FileFormat, PartitionSpec, Schema}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Path}

class IcebergRecordBatchBuilderTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private val testDir: Path = Files.createTempDirectory("iceberg_unit_tests")

  private val catalog = new HadoopCatalog(new Configuration(), testDir.toAbsolutePath.toString)
  private val schema = new Schema(
    NestedField.required(1, "id", Types.LongType.get()),
    NestedField.optional(2, "name", Types.StringType.get())
  )
  private val partitionSpec = PartitionSpec.builderFor(schema).truncate("name", 1).build()

  def newBuilder(tableName: String): IcebergRecordBatchBuilder = new IcebergRecordBatchBuilder(
    catalog.createTable(TableIdentifier.of("test", tableName), schema, partitionSpec),
    (r: StreamRecord) => {
      val result = GenericRecord.create(schema)
      result.setField("id", r.consumerRecord.offset())
      result.setField("name", new String(r.consumerRecord.value(), "UTF-8"))
      Seq(result)
    },
    FileFormat.PARQUET,
    MultiFileCommitStrategy.anyFile(ReachedAnyOf(recordsWritten = Some(3))),
    Map.empty
  )

  describe("IcebergRecordBatchBuilder") {

    val builder = newBuilder("commit")

    it("should commit not commit records until some partition is full") {
      builder.add(newStreamRecord("test", 0, 1, Timestamp(1), "key", "xx"))
      builder.add(newStreamRecord("test", 0, 2, Timestamp(2), "key", "xy"))
      builder.add(newStreamRecord("test", 0, 3, Timestamp(3), "key", "zz"))

      builder.isBatchReady shouldEqual false
    }

    it("should commit records after some partitions is full") {
      builder.add(newStreamRecord("test", 0, 4, Timestamp(4), "key", "xz"))

      builder.isBatchReady shouldEqual true
    }

    it("should create directories for all partitions") {
      builder.build()

      new File(s"${testDir.toAbsolutePath}/test/commit/data/name_trunc=x").exists() shouldBe true
      new File(s"${testDir.toAbsolutePath}/test/commit/data/name_trunc=z").exists() shouldBe true
    }
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(testDir.toFile)
  }
}
