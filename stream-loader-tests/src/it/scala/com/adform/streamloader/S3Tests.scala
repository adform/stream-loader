/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.behaviors.{BasicLoaderBehaviors, KafkaRestartBehaviors, RebalanceBehaviors}
import com.adform.streamloader.fixtures._
import com.adform.streamloader.loaders.{TestGroupingS3Loader, TestS3Loader}
import com.adform.streamloader.storage.{LoaderS3Config, S3StorageBackend}
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers

import scala.concurrent.ExecutionContext

class S3Tests
    extends AnyFunSpec
    with Matchers
    with Eventually
    with Checkers
    with DockerTestFixture
    with KafkaTestFixture
    with S3TestFixture
    with Loaders
    with BasicLoaderBehaviors
    with RebalanceBehaviors
    with KafkaRestartBehaviors {

  implicit val context: ExecutionContext = ExecutionContext.global

  val kafkaConfig: KafkaConfig = KafkaConfig()
  val s3Config: S3Config = S3Config()

  def s3Backend(loader: Loader)(testId: String): S3StorageBackend = {
    val s3Bucket = testId.replace("_", "-")
    val backend = S3StorageBackend(
      docker,
      dockerNetwork,
      kafkaContainer,
      s3Container,
      LoaderS3Config(accessKey, secretKey, s3Bucket),
      s3Client,
      loader
    )
    backend.initialize()
    backend
  }

  it should behave like basicLoader("S3 file loader", s3Backend(TestS3Loader))

  // Rebalance tests

  it should behave like rebalancingLoader("S3 file loader", s3Backend(TestS3Loader))

  it should behave like rebalancingLoader("S3 grouping file loader", s3Backend(TestGroupingS3Loader))

  // Kafka restart test

  it should behave like restartKafka("S3 file loader", s3Backend(TestS3Loader))
}
