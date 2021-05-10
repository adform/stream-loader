/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import java.util.UUID

import com.adform.streamloader.batch.storage.RecordBatchStorage
import com.adform.streamloader.fixtures.{Container, ContainerWithEndpoint, DockerNetwork, SimpleContainer}
import com.adform.streamloader.loaders.{TestExternalOffsetVerticaLoader, TestInRowOffsetVerticaLoader}
import com.adform.streamloader.model.{ExampleMessage, RecordBatch, StreamPosition, Timestamp}
import com.adform.streamloader.util.Retry
import com.adform.streamloader.vertica.{ExternalOffsetVerticaFileStorage, InRowOffsetVerticaFileStorage}
import com.adform.streamloader.{BuildInfo, Loader}
import com.spotify.docker.client.DockerClient
import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}
import com.zaxxer.hikari.HikariConfig
import javax.sql.DataSource
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Arbitrary

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Using

abstract class VerticaStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    verticaContainer: ContainerWithEndpoint,
    verticaConf: HikariConfig,
    dataSource: DataSource,
    table: String
) extends StorageBackend[ExampleMessage]
    with JdbcStorageBackend {

  override def arbMessage: Arbitrary[ExampleMessage] = ExampleMessage.arbMessage

  val OFFSET_TABLE = "offsets"
  val FILE_ID_SEQUENCE = "file_ids"

  val FILE_ID_COLUMN = "_file_id"
  val CONSUMER_GROUP_COLUMN = "_consumer_group"
  val TOPIC_COLUMN = "_topic"
  val PARTITION_COLUMN = "_partition"
  val START_OFFSET_COLUMN = "_start_offset"
  val START_WATERMARK_COLUMN = "_start_watermark"
  val END_OFFSET_COLUMN = "_end_offset"
  val END_WATERMARK_COLUMN = "_end_watermark"

  val OFFSET_COLUMN = "_offset"
  val WATERMARK_COLUMN = "_watermark"

  val CONTENT_COLUMN_CREATE_SQL: String =
    s"""  id INT NOT NULL,
       |  name VARCHAR(500) NOT NULL,
       |  timestamp TIMESTAMP NOT NULL,
       |  height FLOAT NOT NULL,
       |  width FLOAT NOT NULL,
       |  is_enabled BOOLEAN NOT NULL,
       |  child_ids VARCHAR(65000) NOT NULL,
       |  parent_id INT,
       |  transaction_id UUID NOT NULL,
       |  money_spent DECIMAL(${ExampleMessage.SCALE_PRECISION.precision}, ${ExampleMessage.SCALE_PRECISION.scale}) NOT NULL
       |""".stripMargin

  def getBatchStorage: RecordBatchStorage[RecordBatch]

  def createLoaderContainer(loader: Loader, loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container = {
    val consumerGroup = loaderKafkaConfig.consumerGroup
    val topic = loaderKafkaConfig.topic
    val loaderName = s"vertica-loader-${UUID.randomUUID().toString.take(6)}"

    val config = ContainerConfig
      .builder()
      .image(BuildInfo.dockerImage)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .build()
      )
      .env(
        s"APP_MAIN_CLASS=${loader.getClass.getName.replace("$", "")}",
        "APP_OPTS=-Dconfig.resource=application-vertica.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=$topic",
        s"KAFKA_CONSUMER_GROUP=$consumerGroup",
        s"VERTICA_PASSWORD=${verticaConf.getDataSourceProperties.getProperty("password")}",
        s"VERTICA_PORT=${verticaConf.getDataSourceProperties.get("port").asInstanceOf[Short]}",
        s"VERTICA_USER=${verticaConf.getDataSourceProperties.getProperty("userID")}",
        s"VERTICA_HOST=${verticaConf.getDataSourceProperties.getProperty("host")}",
        s"VERTICA_DB=${verticaConf.getDataSourceProperties.getProperty("database")}",
        s"LOADER_TABLE=$table",
        s"LOADER_OFFSET_TABLE=$OFFSET_TABLE",
        s"LOADER_FILE_ID_SEQUENCE=$FILE_ID_SEQUENCE",
        s"BATCH_SIZE=$batchSize"
      )
      .build()

    val containerCreation = docker.createContainer(config, loaderName)
    SimpleContainer(containerCreation.id, loaderName)
  }

  override def committedPositions(
      loaderKafkaConfig: LoaderKafkaConfig,
      partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    val batchStorage = getBatchStorage
    val kafkaContext = getKafkaContext(kafkaContainer, loaderKafkaConfig.consumerGroup)
    batchStorage.initialize(kafkaContext)
    batchStorage.committedPositions(partitions)
  }

  def contentSqlQuery: String

  override def getContent: StorageContent[ExampleMessage] =
    Retry.retryOnFailure(Retry.Policy(retriesLeft = 3, initialDelay = 1.seconds, backoffFactor = 1)) {
      Using.resource(dataSource.getConnection()) { connection =>
        val content = Using.resource(connection.prepareStatement(contentSqlQuery)) { ps =>
          ps.setQueryTimeout(5)
          Using.resource(ps.executeQuery()) { rs =>
            val content: ListBuffer[ExampleMessage] = collection.mutable.ListBuffer[ExampleMessage]()
            val positions: TrieMap[TopicPartition, ListBuffer[StreamPosition]] = TrieMap.empty
            while (rs.next()) {

              content.addOne(
                ExampleMessage(
                  rs.getInt("id"),
                  rs.getString("name"),
                  rs.getTimestamp("timestamp").toLocalDateTime,
                  rs.getDouble("height"),
                  rs.getFloat("width"),
                  rs.getBoolean("is_enabled"),
                  rs.getString("child_ids").split(';').map(_.toInt),
                  Option(rs.getObject("parent_id").asInstanceOf[java.lang.Long]).map(_.toLong),
                  rs.getObject("transaction_id").asInstanceOf[UUID],
                  rs.getBigDecimal("money_spent")
                ))

              val topicPartition = new TopicPartition(rs.getString(TOPIC_COLUMN), rs.getInt(PARTITION_COLUMN))
              val position =
                StreamPosition(rs.getLong(OFFSET_COLUMN), Timestamp(rs.getTimestamp(WATERMARK_COLUMN).getTime))
              positions
                .getOrElseUpdate(topicPartition, collection.mutable.ListBuffer[StreamPosition]())
                .append(position)
            }

            StorageContent(content.toList, positions.view.mapValues(sps => sps.maxBy(_.offset)).toMap)
          }
        }
        content
      }
    }
}

case class ExternalOffsetVerticaStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    verticaContainer: ContainerWithEndpoint,
    verticaConf: HikariConfig,
    dataSource: DataSource,
    table: String)
    extends VerticaStorageBackend(
      docker,
      dockerNetwork,
      kafkaContainer,
      verticaContainer,
      verticaConf,
      dataSource,
      table) {

  override def getBatchStorage: RecordBatchStorage[RecordBatch] =
    ExternalOffsetVerticaFileStorage
      .builder()
      .dbDataSource(dataSource)
      .table(table)
      .offsetTable(OFFSET_TABLE)
      .offsetTableColumnNames(
        FILE_ID_COLUMN,
        CONSUMER_GROUP_COLUMN,
        TOPIC_COLUMN,
        PARTITION_COLUMN,
        START_OFFSET_COLUMN,
        START_WATERMARK_COLUMN,
        END_OFFSET_COLUMN,
        END_WATERMARK_COLUMN)
      .build()
      .asInstanceOf[RecordBatchStorage[RecordBatch]]

  override def initialize(): Unit = {
    executeStatement(s"CREATE SEQUENCE IF NOT EXISTS $FILE_ID_SEQUENCE")
    executeStatement(s"""CREATE TABLE IF NOT EXISTS $OFFSET_TABLE(
                        |  $FILE_ID_COLUMN INT NOT NULL,
                        |  $CONSUMER_GROUP_COLUMN VARCHAR(1024) NOT NULL,
                        |  $TOPIC_COLUMN VARCHAR(128) NOT NULL,
                        |  $PARTITION_COLUMN INT NOT NULL,
                        |  $START_OFFSET_COLUMN INT NOT NULL,
                        |  $START_WATERMARK_COLUMN TIMESTAMP NOT NULL,
                        |  $END_OFFSET_COLUMN INT NOT NULL,
                        |  $END_WATERMARK_COLUMN TIMESTAMP NOT NULL
                        |);
       """.stripMargin)

    executeStatement(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  $FILE_ID_COLUMN INT NOT NULL,
         |  $CONTENT_COLUMN_CREATE_SQL
         |);""".stripMargin
    )
  }

  override def contentSqlQuery: String =
    s"""SELECT
       |  content.*,
       |  offsets.$TOPIC_COLUMN,
       |  offsets.$PARTITION_COLUMN,
       |  offsets.$END_OFFSET_COLUMN AS $OFFSET_COLUMN,
       |  offsets.$END_WATERMARK_COLUMN AS $WATERMARK_COLUMN
       |FROM $table content
       |LEFT JOIN $OFFSET_TABLE offsets ON content.$FILE_ID_COLUMN = offsets.$FILE_ID_COLUMN
      """.stripMargin

  override def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container =
    createLoaderContainer(TestExternalOffsetVerticaLoader, loaderKafkaConfig, batchSize)
}

case class InRowOffsetVerticaStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    verticaContainer: ContainerWithEndpoint,
    verticaConf: HikariConfig,
    dataSource: DataSource,
    table: String)
    extends VerticaStorageBackend(
      docker,
      dockerNetwork,
      kafkaContainer,
      verticaContainer,
      verticaConf,
      dataSource,
      table) {

  override def getBatchStorage: RecordBatchStorage[RecordBatch] =
    InRowOffsetVerticaFileStorage
      .builder()
      .dbDataSource(dataSource)
      .table(table)
      .rowOffsetColumnNames(TOPIC_COLUMN, PARTITION_COLUMN, OFFSET_COLUMN, WATERMARK_COLUMN)
      .build()
      .asInstanceOf[RecordBatchStorage[RecordBatch]]

  override def initialize(): Unit = {
    executeStatement(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  $TOPIC_COLUMN VARCHAR(500) NOT NULL,
         |  $PARTITION_COLUMN INT NOT NULL,
         |  $OFFSET_COLUMN INT NOT NULL,
         |  $WATERMARK_COLUMN TIMESTAMP NOT NULL,
         |  $CONTENT_COLUMN_CREATE_SQL
         |);""".stripMargin
    )
  }

  override def contentSqlQuery: String = s"SELECT * FROM $table"

  override def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container =
    createLoaderContainer(TestInRowOffsetVerticaLoader, loaderKafkaConfig, batchSize)
}
