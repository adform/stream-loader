/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import java.net.URI

import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.log4s._
import org.scalatest.{BeforeAndAfterAll, Suite}

case class HdfsConfig(
    namenodeImage: String = "bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8",
    datanodeImage: String = "bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8"
)

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
        s"CLUSTER_NAME=$dockerSandboxId-integration-test",
        s"CORE_CONF_fs_defaultFS=hdfs://$nameNodeName:$hdfsPort"
      )
    )

    datanode = startDataNode(dockerNetwork)(
      s"$dockerSandboxId-hadoop-datanode",
      List(
        s"CORE_CONF_fs_defaultFS=hdfs://${namenode.name}:$hdfsPort",
        s"CORE_CONF_dfs_datanode_address=${dockerNetwork.ip}:$datanodePort"
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

  private def startDataNode(network: DockerNetwork)(
      containerName: String,
      envVars: List[String] = List(),
      links: List[String] = List()): Container = {
    val config = ContainerConfig
      .builder()
      .image(hdfsConfig.datanodeImage)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(network.id)
          .portBindings(makePortBindings(datanodePort))
          .build()
      )
      .exposedPorts(datanodePort.toString)
      .env(envVars: _*)
      .build()

    val containerId = startContainer(config, containerName)
    val container = SimpleContainer(containerId, containerName)

    log.info(s"Waiting for HDFS datanode ($containerName) to become healthy")
    ensureHealthy(container)

    container
  }

  private def startNameNode(
      network: DockerNetwork)(containerName: String, envVars: List[String] = List()): ContainerWithEndpoint = {
    val config = ContainerConfig
      .builder()
      .image(hdfsConfig.namenodeImage)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(network.id)
          .portBindings(makePortBindings(hdfsPort))
          .build()
      )
      .exposedPorts(hdfsPort.toString)
      .env(envVars: _*)
      .build()

    val containerId = startContainer(config, containerName)
    val container = GenericContainer(containerId, containerName, network.ip, hdfsPort)

    log.info(s"Waiting for HDFS namenode ($containerName) to become healthy")
    ensureHealthy(container)

    container
  }
}
