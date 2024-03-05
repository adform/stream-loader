/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.behaviors

import java.util.UUID

import com.adform.streamloader.fixtures.{Docker, Kafka, Loaders}
import com.adform.streamloader.model.StorageMessage
import com.adform.streamloader.storage.{LoaderKafkaConfig, StorageBackend}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

trait KafkaRestartBehaviors {
  this: AnyFunSpec with Matchers with Eventually with Docker with Kafka with Loaders =>

  private def genTestId: String = UUID.randomUUID().toString.take(6)

  def restartKafka[M <: StorageMessage](testPrefix: String, storageBackendFactory: String => StorageBackend[M])(implicit
      ec: ExecutionContext
  ): Unit = {

    it(s"$testPrefix should keep loading messages after Kafka restarts") {

      val testName = s"kafka_restart_test_$genTestId"
      val (topic, partition, consumerGroup) = (s"topic_$testName", 0, s"consumer_group_$testName")
      val tp = new TopicPartition(topic, partition)
      val backend = storageBackendFactory(topic)

      withKafkaTopics(new NewTopic(topic, 1, 1.asInstanceOf[Short])) {
        withKafkaProducer { producer =>
          // Test plan:
          // 1. Load some messages
          // 2. Restart Kafka container
          // 3. Send more messages
          // 4. Confirm that eventually everything gets loaded

          // 1. Load some messages

          val messages = backend.generateRandomMessages(5, seed = 1)
          val loaderKafkaConfig = LoaderKafkaConfig(consumerGroup, topic)

          // Load messages
          withLoader(backend)(
            loaderKafkaConfig,
            messages.length
          ) { _ =>
            sendMessagesAndConfirm(backend)(
              producer,
              loaderKafkaConfig,
              tp,
              messages,
              messageBatchCount = 1
            )

            // Confirm content
            eventually {
              backend.getContent.messages should contain theSameElementsAs messages
            }

            // 2. Restart Kafka container

            docker.restartContainer(kafkaContainer.id)

            // 3. Send more messages

            sendMessagesAndConfirm(backend)(
              producer,
              loaderKafkaConfig,
              tp,
              messages,
              messageBatchCount = 1
            )

            // 4. Confirm that eventually everything gets loaded
            eventually {
              backend.getContent.messages should contain theSameElementsAs (messages ++ messages)
            }
          }
        }
      }
    }
  }
}
