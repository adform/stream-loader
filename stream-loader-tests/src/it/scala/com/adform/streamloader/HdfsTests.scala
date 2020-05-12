/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.behaviors.BasicLoaderBehaviors
import com.adform.streamloader.fixtures._
import com.adform.streamloader.loaders.TestParquetHdfsLoader
import com.adform.streamloader.storage.HdfsStorageBackend
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers

import scala.concurrent.ExecutionContext

class HdfsTests
    extends AnyFunSpec
    with Checkers
    with Matchers
    with Eventually
    with DockerTestFixture
    with KafkaTestFixture
    with HdfsTestFixture
    with Loaders
    with BasicLoaderBehaviors {

  implicit val context: ExecutionContext = ExecutionContext.global

  val kafkaConfig: KafkaConfig = KafkaConfig()
  val hdfsConfig: HdfsConfig = HdfsConfig()

  def hdfsBackend(testId: String): HdfsStorageBackend = {
    val backend =
      HdfsStorageBackend(
        docker,
        dockerNetwork,
        kafkaContainer,
        namenodeContainer,
        datanodeContainer,
        hadoopFs,
        baseDir = s"/$testId",
        TestParquetHdfsLoader
      )
    backend.initialize()
    backend
  }

  it should behave like basicLoader("HDFS parquet file loader", hdfsBackend)
}
