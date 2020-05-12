/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.s3

import java.net.{ServerSocket, URI}
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.locks.ReentrantLock

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import org.eclipse.jetty.util.component.AbstractLifeCycle
import org.gaul.s3proxy.{AuthenticationType, S3Proxy}
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

trait MockS3 extends BeforeAndAfterAll { this: AnyFunSpec =>

  private lazy val randomPort = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
  private lazy val endpoint = new EndpointConfiguration(s"http://localhost:$randomPort", "test")
  private val (accessKey, secretKey) = ("access", "secret")

  private val blobStoreConfig = new Properties() {
    put("jclouds.filesystem.basedir", Files.createTempDirectory("S3ProxyRule").toFile.getCanonicalPath)
  }

  private val blobStoreContext = ContextBuilder
    .newBuilder("filesystem")
    .overrides(blobStoreConfig)
    .build(classOf[BlobStoreContext])

  private lazy val s3Mock: S3Proxy = S3Proxy
    .builder()
    .endpoint(new URI(endpoint.getServiceEndpoint))
    .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, accessKey, secretKey)
    .blobStore(blobStoreContext.getBlobStore)
    .build()

  lazy val s3Client: S3Client = S3Client
    .builder()
    .region(Region.US_EAST_1)
    .credentialsProvider(() => AwsBasicCredentials.create(accessKey, secretKey))
    .endpointOverride(new URI(endpoint.getServiceEndpoint))
    .build()

  override def beforeAll(): Unit = {
    MockS3.lock.lock()
    s3Mock.start()
    while (s3Mock.getState != AbstractLifeCycle.STARTED) {
      Thread.sleep(10)
    }
    MockS3.lock.unlock()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    s3Client.close()
    s3Mock.stop()
    super.afterAll()
  }
}

object MockS3 {
  // Lock to avoid race conditions when finding a free port and creating the mock service upon startup.
  val lock = new ReentrantLock()
}
