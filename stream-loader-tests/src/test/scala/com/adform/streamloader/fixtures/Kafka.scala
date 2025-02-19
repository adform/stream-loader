/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import org.log4s.getLogger
import org.mandas.docker.client.messages.{ContainerConfig, Healthcheck, HostConfig}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, TimeoutException}
import java.util.{Properties, UUID}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

case class KafkaConfig(image: String = "bitnami/kafka:3.9.0-debian-12-r3")

trait KafkaTestFixture extends Kafka with BeforeAndAfterAll with BeforeAndAfterEach {
  this: Suite with DockerTestFixture =>
  override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaInit()
  }

  override def beforeEach(): Unit = {
    ensureKafkaHealthy()
  }

  override def afterAll(): Unit =
    try kafkaCleanup()
    finally super.afterAll()
}

trait Kafka { this: Docker =>

  def kafkaConfig: KafkaConfig

  private[this] val log = getLogger

  private val kafkaPort = 9092
  private val kafkaControllerPort = 9093

  private val timeout = 30
  private val timeoutUnit = TimeUnit.SECONDS

  private var kafka: ContainerWithEndpoint = _

  def kafkaInit(): Unit = {
    kafka = startKafkaContainer()
  }

  def kafkaCleanup(): Unit =
    if (kafkaContainer != null) {
      log.info(s"Stopping and removing the Kafka container ${kafkaContainer.name}")
      kafkaStop()
    }

  def kafkaStop(): Unit = stopAndRemoveContainer(kafka)

  def kafkaContainer: ContainerWithEndpoint = kafka

  def withConsumedRecords[T](tps: TopicPartition*)(code: Iterable[ConsumerRecord[Array[Byte], Array[Byte]]] => T): T = {
    val buffer = ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
    val running = new AtomicBoolean(true)

    val consumptionThread = new Thread(() =>
      withKafkaConsumer() { consumer =>
        consumer.assign(tps.asJava)
        while (running.get())
          consumer
            .poll(Duration.ofMillis(100))
            .forEach(buffer.append(_))
      }
    )

    try {
      consumptionThread.start()
      code(buffer)
    } finally {
      running.set(false)
      consumptionThread.join()
    }
  }

  def withKafkaAdminClient[T](code: AdminClient => T): T =
    Using(createKafkaAdminClient())(code).get

  def withKafkaConsumer[T](groupId: String = UUID.randomUUID().toString)(
      code: KafkaConsumer[Array[Byte], Array[Byte]] => T
  ): T =
    Using(createKafkaConsumer(groupId))(code).get

  def withKafkaProducer[T](code: KafkaProducer[Array[Byte], Array[Byte]] => T): T =
    Using(createKafkaProducer())(code).get

  def withKafkaTopics[T](newTopics: NewTopic*)(code: => T): T =
    withKafkaAdminClient { client =>
      client.createTopics(newTopics.asJava).all().get(timeout, timeoutUnit)

      val result = code

      client.deleteTopics(newTopics.map(_.name).asJava)
      result
    }

  def ensureKafkaHealthy(timeout: Duration = Duration.ofSeconds(60)): Unit = {
    try {
      withKafkaAdminClient { client =>
        client.listTopics().names().get(timeout.toMillis, TimeUnit.MILLISECONDS) // 60 seconds
      }
    } catch {
      case _: TimeoutException => log.warn("Kafka is unhealthy")
    }
  }

  private def startKafkaContainer(): ContainerWithEndpoint = {
    val kafkaName = s"$dockerSandboxId-kafka"
    val config = ContainerConfig
      .builder()
      .image(kafkaConfig.image)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .portBindings(makePortBindings(kafkaPort))
          .build()
      )
      .healthcheck(
        Healthcheck
          .builder()
          .test(
            List(
              "CMD-SHELL",
              s"/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:$kafkaPort"
            ).asJava
          )
          .retries(6)
          .interval(1_000_000_000L)
          .timeout(5_000_000_000L)
          .build()
      )
      .exposedPorts(kafkaPort.toString)
      .env(
        "KAFKA_CFG_NODE_ID=1",
        "KAFKA_CFG_PROCESS_ROLES=broker,controller",
        "KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        s"KAFKA_CFG_LISTENERS=PLAINTEXT://:$kafkaPort,CONTROLLER://:$kafkaControllerPort",
        "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        s"KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://${dockerNetwork.ip}:$kafkaPort",
        s"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:$kafkaControllerPort",
        "ALLOW_PLAINTEXT_LISTENER=yes",
        s"KAFKA_CFG_LOG_RETENTION_HOURS=${Int.MaxValue}"
      )
      .build()

    val containerId = startContainer(config, kafkaName)
    val container = GenericContainer(containerId, kafkaName, dockerNetwork.ip, kafkaPort)

    log.info(s"Waiting for Kafka ($kafkaName) to become healthy")
    ensureHealthy(container)

    container
  }

  private def createKafkaAdminClient(): AdminClient = {
    val props: Properties = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.endpoint)
    props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "600000")
    AdminClient.create(props)
  }

  private def createKafkaProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.endpoint)
    producerProps.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName
    )
    producerProps.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName
    )
    producerProps.put(ProducerConfig.RETRIES_CONFIG, "10")
    new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
  }

  private def createKafkaConsumer(groupId: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.endpoint)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName
    )
    consumerProps.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName
    )
    new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  }
}
