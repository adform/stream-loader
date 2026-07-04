/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.log4s._
import org.mandas.docker.client.messages.{ContainerConfig, Healthcheck, HostConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.net.URI
import scala.jdk.CollectionConverters._

case class HdfsConfig(image: String = "apache/hadoop:3.4.1")

trait HdfsTestFixture extends Hdfs with BeforeAndAfterAll { this: Suite with DockerTestFixture =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsInit()
  }

  override def afterAll(): Unit =
    try hdfsCleanup()
    finally super.afterAll()
}

trait Hdfs { this: Docker =>

  def hdfsConfig: HdfsConfig

  private[this] val log = getLogger

  private val hdfsPort = 8020
  private val datanodePort = 50010

  private var namenode: ContainerWithEndpoint = _
  private var datanode: Container = _
  private var hdfs: FileSystem = _

  def hdfsInit(): Unit = {
    val nameNodeName = s"$dockerSandboxId-hadoop-namenode"

    System.setProperty("HADOOP_USER_NAME", "root")

    namenode = startNameNode(dockerNetwork)(
      nameNodeName,
      List(
        "HADOOP_USER_NAME=root",
        "ENSURE_NAMENODE_DIR=/tmp/hadoop-root/dfs/name",
        s"CORE-SITE.XML_fs.defaultFS=hdfs://$nameNodeName:$hdfsPort",
        "HDFS-SITE.XML_dfs.namenode.rpc-bind-host=0.0.0.0"
      )
    )

    datanode = startDataNode(dockerNetwork)(
      s"$dockerSandboxId-hadoop-datanode",
      List(
        s"CORE-SITE.XML_fs.defaultFS=hdfs://${namenode.name}:$hdfsPort",
        s"HDFS-SITE.XML_dfs.datanode.address=0.0.0.0:$datanodePort"
      ),
      List(namenode.name)
    )

    val conf = new Configuration()
    val uri = s"hdfs://${namenode.endpoint}"
    conf.set("fs.defaultFS", uri)
    hdfs = FileSystem.get(new URI(uri), conf)
  }

  def hdfsCleanup(): Unit = {
    if (datanodeContainer != null) {
      log.info(s"Stopping and removing the HDFS datanode ${datanodeContainer.name}")
      stopAndRemoveContainer(datanodeContainer)
    }
    if (namenodeContainer != null) {
      log.info(s"Stopping and removing the HDFS namenode ${namenodeContainer.name}")
      stopAndRemoveContainer(namenodeContainer)
    }
    if (hadoopFs != null) {
      log.info(s"Closing HDFS client")
      hadoopFs.close()
    }
  }

  def namenodeContainer: ContainerWithEndpoint = namenode
  def datanodeContainer: Container = datanode
  def hadoopFs: FileSystem = hdfs

  private def startDataNode(
      network: DockerNetwork
  )(containerName: String, envVars: List[String] = List(), links: List[String] = List()): Container = {
    val config = ContainerConfig
      .builder()
      .image(hdfsConfig.image)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(network.id)
          .portBindings(makePortBindings(datanodePort))
          .build()
      )
      .healthcheck(
        Healthcheck
          .builder()
          .test(List("CMD-SHELL", s"nc -z localhost $datanodePort").asJava)
          .retries(6)
          .interval(1_000_000_000L)
          .timeout(5_000_000_000L)
          .build()
      )
      .exposedPorts(datanodePort.toString)
      .env(envVars: _*)
      .cmd(Seq("hdfs", "datanode").asJava)
      .build()

    val containerId = startContainer(config, containerName)
    val container = SimpleContainer(containerId, containerName)

    log.info(s"Waiting for HDFS datanode ($containerName) to become healthy")
    ensureHealthy(container)

    container
  }

  private def startNameNode(
      network: DockerNetwork
  )(containerName: String, envVars: List[String] = List()): ContainerWithEndpoint = {
    val config = ContainerConfig
      .builder()
      .image(hdfsConfig.image)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(network.id)
          .portBindings(makePortBindings(hdfsPort))
          .build()
      )
      .healthcheck(
        Healthcheck
          .builder()
          .test(List("CMD-SHELL", s"nc -z localhost $hdfsPort").asJava)
          .retries(6)
          .interval(1_000_000_000L)
          .timeout(5_000_000_000L)
          .build()
      )
      .exposedPorts(hdfsPort.toString)
      .env(envVars: _*)
      .cmd(Seq("hdfs", "namenode").asJava)
      .build()

    val containerId = startContainer(config, containerName)
    val container = GenericContainer(containerId, containerName, network.ip, hdfsPort)

    log.info(s"Waiting for HDFS namenode ($containerName) to become healthy")
    ensureHealthy(container)

    container
  }
}
