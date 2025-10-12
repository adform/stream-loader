/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.behaviors.{BasicLoaderBehaviors, RebalanceBehaviors}
import com.adform.streamloader.fixtures._
import com.adform.streamloader.loaders.TestClickHouseLoader
import com.adform.streamloader.storage.ClickHouseStorageBackend
import com.clickhouse.client.api.Client
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow
import org.scalatestplus.scalacheck.Checkers

import scala.concurrent.ExecutionContext

@Slow
class ClickHouseIntegrationTests
    extends AnyFunSpec
    with Matchers
    with Eventually
    with Checkers
    with DockerTestFixture
    with KafkaTestFixture
    with Loaders
    with ClickHouseTestFixture
    with BasicLoaderBehaviors
    with RebalanceBehaviors {

  implicit val context: ExecutionContext = ExecutionContext.global

  val kafkaConfig: KafkaConfig = KafkaConfig()
  val clickHouseConfig: ClickHouseConfig = ClickHouseConfig()
  var clickHouseClient: Client = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    clickHouseClient = new Client.Builder()
      .addEndpoint(s"http://${clickHouseContainer.ip}:${clickHouseContainer.port}")
      .setUsername(clickHouseConfig.userName)
      .setPassword(clickHouseConfig.password)
      .setDefaultDatabase(clickHouseConfig.dbName)
      .build()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    clickHouseClient.close()
  }

  def clickHouseStorageBackend(testId: String): ClickHouseStorageBackend = {
    val table = testId.replace("-", "_")
    val backend =
      storage.ClickHouseStorageBackend(
        docker,
        dockerNetwork,
        kafkaContainer,
        clickHouseContainer,
        clickHouseConfig,
        clickHouseClient,
        table,
        TestClickHouseLoader
      )
    backend.initialize()
    backend
  }

  it should behave like basicLoader("ClickHouse RowBinary loader", clickHouseStorageBackend)
}
