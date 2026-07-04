/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import org.log4s.getLogger
import org.mandas.docker.client.messages.{ContainerConfig, Healthcheck, HostConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.time.Duration
import scala.jdk.CollectionConverters._

case class ClickHouseConfig(
    image: String = "clickhouse/clickhouse-server:25.9.3.48",
    dbName: String = "default",
    userName: String = "default",
    password: String = "password"
)

trait ClickHouseTestFixture extends ClickHouse with BeforeAndAfterAll { this: Suite with DockerTestFixture =>
  override def beforeAll(): Unit = {
    super.beforeAll()
    clickHouseInit()
  }

  override def afterAll(): Unit =
    try clickHouseCleanUp()
    finally super.afterAll()
}

trait ClickHouse { this: Docker =>
  def clickHouseConfig: ClickHouseConfig

  private[this] val log = getLogger

  val jdbcPort = 8123
  val nativeClientPort = 9000

  private var clickHouse: ContainerWithEndpoint = _

  override val healthCheckTimeout: Duration = Duration.ofMinutes(3)

  def clickHouseContainer: ContainerWithEndpoint = clickHouse

  def clickHouseInit(): Unit = {
    clickHouse = startClickHouseContainer()
  }

  def clickHouseCleanUp(): Unit =
    if (clickHouseContainer != null) {
      log.debug(s"Stopping and removing ClickHouse container ${clickHouseContainer.name}")
      stopAndRemoveContainer(clickHouseContainer)
    }

  private def startClickHouseContainer(): ContainerWithEndpoint = {
    val containerName = s"$dockerSandboxId-clickHouse"
    val config = ContainerConfig
      .builder()
      .image(clickHouseConfig.image)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .portBindings(makePortBindings(jdbcPort, nativeClientPort))
          .build()
      )
      .env(
        s"CLICKHOUSE_DB=${clickHouseConfig.dbName}",
        s"CLICKHOUSE_USER=${clickHouseConfig.userName}",
        s"CLICKHOUSE_PASSWORD=${clickHouseConfig.password}",
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1"
      )
      .healthcheck(
        Healthcheck
          .builder()
          .test(List("CMD-SHELL", "clickhouse-client -q 'select 1'").asJava)
          .retries(6)
          .interval(10000000000L)
          .timeout(1000000000L)
          .build()
      )
      .exposedPorts(jdbcPort.toString, nativeClientPort.toString)
      .build()

    val containerId = startContainer(config, containerName)
    val container = GenericContainer(containerId, containerName, dockerNetwork.ip, jdbcPort)

    log.info(s"Waiting for ClickHouse ($containerName) to become healthy")
    ensureHealthy(container)

    container
  }
}
