/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import java.time.Duration
import java.util.UUID

import org.mandas.docker.client.DefaultDockerClient
import org.mandas.docker.client.DockerClient.RemoveContainerParam
import org.mandas.docker.client.builder.resteasy.ResteasyDockerClientBuilder
import org.mandas.docker.client.messages.{ContainerConfig, NetworkConfig, PortBinding}
import org.log4s.getLogger
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.jdk.CollectionConverters._

case class DockerNetwork(id: String, name: String, ip: String)

trait Container {
  def id: String
  def name: String
}

trait ContainerWithEndpoint extends Container {
  def ip: String
  def port: Int
  def endpoint: String = s"$ip:$port"
}

case class SimpleContainer(id: String, name: String) extends Container
case class GenericContainer(id: String, name: String, ip: String, port: Int) extends ContainerWithEndpoint

trait DockerTestFixture extends Docker with BeforeAndAfterAll { this: Suite =>
  override def beforeAll(): Unit = {
    super.beforeAll()
    dockerInit()
  }

  override def afterAll(): Unit =
    try dockerCleanup()
    finally super.afterAll()
}

trait Docker {

  private[this] val log = getLogger

  private var network: DockerNetwork = _

  val docker: DefaultDockerClient = new ResteasyDockerClientBuilder().fromEnv().build()
  val dockerSandboxId: String = UUID.randomUUID().toString

  val healthCheckTimeout: Duration = Duration.ofSeconds(60)
  val healthCheckInterval: Duration = Duration.ofSeconds(1)

  def dockerInit(): Unit = {
    network = createNetwork()
  }

  def dockerCleanup(): Unit =
    if (dockerNetwork != null) {
      log.debug(s"Removing docker network ${dockerNetwork.id}")
      val connectedContainers = docker.inspectNetwork(dockerNetwork.id).containers()
      for ((id, _) <- connectedContainers.asScala) {
        docker.disconnectFromNetwork(id, dockerNetwork.id)
      }
      docker.removeNetwork(dockerNetwork.id)
    }

  def dockerNetwork: DockerNetwork = network

  def makePortBindings(ports: Int*): java.util.Map[String, java.util.List[PortBinding]] =
    ports
      .map(port => port.toString -> List(PortBinding.of(network.ip, port)).asJava)
      .toMap
      .asJava

  def withStoppedContainer[T](container: Container, stoppedFor: Duration)(code: => T): T =
    if (isContainerRunning(container)) {
      docker.stopContainer(container.id, 1)
      Thread.sleep(stoppedFor.toMillis)

      val result = code

      docker.startContainer(container.id)

      log.debug(s"Waiting for ${container.name} to become healthy after being stopped and started")
      ensureHealthy(container)

      result
    } else {
      log.warn(s"Container ${container.name} is not running")
      code
    }

  def ensureHealthy(container: Container, timeSpent: Duration = Duration.ofMillis(0)): Unit =
    isContainerHealthy(container) match {
      case Some(isHealthy) =>
        if (!isHealthy) {
          if (timeSpent.compareTo(healthCheckTimeout) < 0) {
            Thread.sleep(healthCheckInterval.toMillis)
            ensureHealthy(container, timeSpent.plus(healthCheckInterval))
          } else log.warn(s"Container ${container.name} is unhealthy")
        }
      case None => log.warn(s"Container ${container.name} does not have a health check")
    }

  def ensureImage(image: String): Unit = {
    val images = docker.listImages().asScala.filter(_.repoTags() != null).flatMap(_.repoTags().asScala)
    if (!images.contains(image) && !images.contains(s"docker.io/$image")) {
      log.info(s"Pulling image $image")
      docker.pull(image)
    }
  }

  def startContainer(config: ContainerConfig, containerName: String): String = {
    ensureImage(config.image())

    val containerCreation = docker.createContainer(config, containerName)
    docker.startContainer(containerCreation.id())

    containerCreation.id()
  }

  def isContainerHealthy(container: Container): Option[Boolean] =
    Option(
      docker
        .inspectContainer(container.id)
        .state()
        .health()
    ).map(_.status() == "healthy")

  private def createNetwork(): DockerNetwork = {
    val networkName = s"${dockerSandboxId}_network"
    val networkId = docker.createNetwork(NetworkConfig.builder().name(networkName).build()).id()
    val ip = docker.inspectNetwork(networkId).ipam().config().asScala.head.gateway().split('/').head

    log.info(s"Created docker network $networkId on $ip")
    DockerNetwork(networkId, networkName, ip)
  }

  def withContainer[T](container: Container)(code: => T): T =
    try {
      docker.startContainer(container.id)
      code
    } finally stopAndRemoveContainer(container)

  def isContainerRunning(container: Container): Boolean =
    docker.inspectContainer(container.id).state().running()

  def stopAndRemoveContainer(container: Container): Unit = {
    if (isContainerRunning(container)) docker.stopContainer(container.id, 2)
    docker.removeContainer(container.id, RemoveContainerParam.removeVolumes())
  }
}
