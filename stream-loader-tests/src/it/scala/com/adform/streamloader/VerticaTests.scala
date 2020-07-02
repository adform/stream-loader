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
import com.adform.streamloader.storage._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers

import scala.concurrent.ExecutionContext

class VerticaTests
    extends AnyFunSpec
    with Matchers
    with Eventually
    with Checkers
    with DockerTestFixture
    with KafkaTestFixture
    with VerticaTestFixture
    with Loaders
    with BasicLoaderBehaviors {

  implicit val context: ExecutionContext = ExecutionContext.global

  val kafkaConfig: KafkaConfig = KafkaConfig()
  val verticaConfig: VerticaConfig = VerticaConfig()

  var hikariConf: HikariConfig = _
  var dataSource: HikariDataSource = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    hikariConf = new HikariConfig()
    hikariConf.setDataSourceClassName(classOf[com.vertica.jdbc.DataSource].getName)
    hikariConf.addDataSourceProperty("host", verticaContainer.ip)
    hikariConf.addDataSourceProperty("port", verticaContainer.port.asInstanceOf[Short])
    hikariConf.addDataSourceProperty("database", verticaConfig.dbName)
    hikariConf.addDataSourceProperty("userID", "dbadmin")
    hikariConf.addDataSourceProperty("password", "")
    hikariConf.setConnectionTimeout(1000)
    hikariConf.setValidationTimeout(1000)
    hikariConf.setConnectionTestQuery("SELECT 1")

    hikariConf.setMinimumIdle(4)
    hikariConf.setMaximumPoolSize(8)

    dataSource = new HikariDataSource(hikariConf)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dataSource.close()
  }

  def verticaExternalStorageBackend(testId: String): VerticaStorageBackend[Long] = {
    val table = testId.replace("-", "_")
    val backend =
      ExternalOffsetVerticaStorageBackend(
        docker,
        dockerNetwork,
        kafkaContainer,
        verticaContainer,
        hikariConf,
        dataSource,
        table)
    backend.initialize()
    backend
  }

  def verticaInRowStorageBackend(testId: String): VerticaStorageBackend[Unit] = {
    val table = testId.replace("-", "_")
    val backend =
      InRowOffsetVerticaStorageBackend(
        docker,
        dockerNetwork,
        kafkaContainer,
        verticaContainer,
        hikariConf,
        dataSource,
        table)
    backend.initialize()
    backend
  }

  it should behave like basicLoader("Vertica external offset loader", verticaExternalStorageBackend)

  it should behave like basicLoader("Vertica in row offset loader", verticaInRowStorageBackend)
}
