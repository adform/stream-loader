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
import com.adform.streamloader.loaders.TestIcebergLoader
import com.adform.streamloader.storage.IcebergStorageBackend
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow
import org.scalatestplus.scalacheck.Checkers

import scala.concurrent.ExecutionContext

@Slow
class IcebergIntegrationTests
    extends AnyFunSpec
    with Checkers
    with Matchers
    with Eventually
    with DockerTestFixture
    with KafkaTestFixture
    with Loaders
    with BasicLoaderBehaviors {

  implicit val context: ExecutionContext = ExecutionContext.global

  val kafkaConfig: KafkaConfig = KafkaConfig()

  def icebergBackend(testId: String): IcebergStorageBackend = {
    val table = s"test.${testId.replace("-", "_")}"
    val backend =
      IcebergStorageBackend(
        docker,
        dockerNetwork,
        kafkaContainer,
        TestIcebergLoader,
        table
      )
    backend.initialize()
    backend
  }

  it should behave like basicLoader("Iceberg loader", icebergBackend)
}
