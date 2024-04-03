/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import java.net.URI

import org.mandas.docker.client.messages.ContainerConfig.Healthcheck
import org.mandas.docker.client.messages.{ContainerConfig, HostConfig}
import org.log4s.getLogger
import org.scalatest.{BeforeAndAfterAll, Suite}
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import scala.jdk.CollectionConverters._

case class S3Config(image: String = "minio/minio:RELEASE.2024-03-05T04-48-44Z")

trait S3TestFixture extends S3 with BeforeAndAfterAll { this: Suite with DockerTestFixture =>
  override def beforeAll(): Unit = {
    super.beforeAll()
    s3Init()
  }

  override def afterAll(): Unit =
    try s3Cleanup()
    finally super.afterAll()
}

trait S3 { this: Docker =>

  def s3Config: S3Config

  private[this] val log = getLogger

  private val s3Port = 9000

  private var s3: ContainerWithEndpoint = _
  private var awsS3Client: S3Client = _

  val accessKey = "testAccessKey"
  val secretKey = "testSecretKey"

  def s3Init(): Unit = {
    s3 = startS3Container()
    awsS3Client = createS3Client()
  }

  def s3Cleanup(): Unit = {
    if (s3Container != null) {
      log.info(s"Stopping and removing S3 container ${s3Container.name}")
      stopAndRemoveContainer(s3Container)
    }
    if (awsS3Client != null) {
      log.info(s"Closing down S3 client")
      awsS3Client.close()
    }
  }

  def s3Container: ContainerWithEndpoint = s3
  def s3Client: S3Client = awsS3Client

  private def startS3Container(): ContainerWithEndpoint = {
    val s3Name = s"$dockerSandboxId-s3"
    val config = ContainerConfig
      .builder()
      .image(s3Config.image)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .portBindings(makePortBindings(s3Port))
          .build()
      )
      .healthcheck(
        Healthcheck
          .builder()
          .test(List("CMD-SHELL", "mc ready local").asJava)
          .retries(6)
          .interval(10000000000L)
          .timeout(1000000000L)
          .build()
      )
      .exposedPorts(s3Port.toString)
      .env(
        s"MINIO_ROOT_USER=$accessKey",
        s"MINIO_ROOT_PASSWORD=$secretKey"
      )
      .cmd("server", "/data")
      .build()

    val containerId = startContainer(config, s3Name)
    val container = GenericContainer(containerId, s3Name, dockerNetwork.ip, s3Port)

    log.info(s"Waiting for S3 ($s3Name) to become healthy")
    ensureHealthy(container)

    container
  }

  private def createS3Client(): S3Client =
    S3Client
      .builder()
      .region(Region.EU_WEST_1)
      .credentialsProvider(() => AwsBasicCredentials.create(accessKey, secretKey))
      .endpointOverride(new URI(s"http://${s3.endpoint}"))
      .build()
}
