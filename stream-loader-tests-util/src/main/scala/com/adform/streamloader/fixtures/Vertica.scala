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

import java.sql.DriverManager
import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Using

case class VerticaConfig(
    dbName: String = "docker",
    user: String = "dbadmin",
    password: String = "",
    image: String = "jbfavre/vertica:9.2.0-7_centos-7"
)

trait VerticaTestFixture extends Vertica with BeforeAndAfterAll { this: Suite with DockerTestFixture =>
  override def beforeAll(): Unit = {
    super.beforeAll()
    verticaInit()
  }

  override def afterAll(): Unit =
    try verticaCleanup()
    finally super.afterAll()
}

trait Vertica { this: Docker =>

  def verticaConfig: VerticaConfig

  private[this] val log = getLogger

  private val verticaPort = 5433

  private var vertica: ContainerWithEndpoint = _

  override val healthCheckTimeout: Duration = Duration.ofMinutes(3)

  def verticaInit(): Unit = {
    Class.forName(classOf[com.vertica.jdbc.Driver].getName)
    vertica = startVerticaContainer()

    // Increase the default max connection count
    val connectionString =
      s"jdbc:vertica://${vertica.endpoint}/${verticaConfig.dbName}?user=${verticaConfig.user}&password=${verticaConfig.password}"
    Using.resource(DriverManager.getConnection(connectionString)) { conn =>
      Using.resource(conn.prepareStatement("SELECT SET_CONFIG_PARAMETER('MaxClientSessions', 500);")) { st =>
        st.execute()
      }
    }
  }

  def verticaCleanup(): Unit =
    if (verticaContainer != null) {
      log.debug(s"Stopping and removing Vertica container ${verticaContainer.name}")
      stopAndRemoveContainer(verticaContainer)
    }

  def verticaContainer: ContainerWithEndpoint = vertica

  private def startVerticaContainer(): ContainerWithEndpoint = {
    val vsql = "/opt/vertica/bin/vsql"
    val vsqlPassword = if (verticaConfig.password.nonEmpty) verticaConfig.password else "''"
    val containerName = s"$dockerSandboxId-vertica"
    val config = ContainerConfig
      .builder()
      .image(verticaConfig.image)
      .hostConfig(
        HostConfig
          .builder()
          .ulimits(
            Seq(
              HostConfig.Ulimit.builder().name("nofile").hard(65536).soft(65536).build()
            ).asJava
          )
          .networkMode(dockerNetwork.id)
          .portBindings(makePortBindings(verticaPort))
          .build()
      )
      .healthcheck(
        Healthcheck
          .builder()
          .test(List("CMD-SHELL", s"$vsql -U ${verticaConfig.user} -p $vsqlPassword -c 'SELECT 1'").asJava)
          .retries(6)
          .interval(10000000000L)
          .timeout(1000000000L)
          .build()
      )
      .exposedPorts(verticaPort.toString)
      .build()

    val containerId = startContainer(config, containerName)
    val container = GenericContainer(containerId, containerName, dockerNetwork.ip, verticaPort)

    log.info(s"Waiting for Vertica ($containerName) to become healthy")
    ensureHealthy(container)

    container
  }
}
