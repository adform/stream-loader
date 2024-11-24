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
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

trait BasicLoaderBehaviors {
  this: AnyFunSpec with Matchers with Eventually with Docker with Kafka with Loaders =>

  private def genTestId: String = UUID.randomUUID().toString.take(6)

  def basicLoader[M <: StorageMessage](
      testPrefix: String,
      storageBackendFactory: String => StorageBackend[M],
      batchCount: Int = 1
  )(implicit
      ec: ExecutionContext
  ): Unit = {

    it(s"$testPrefix should load messages from Kafka") {
      val testName = s"simple_test_$genTestId"
      val (topic, partition, consumerGroup) = (s"topic_$testName", 0, s"consumer_group_$testName")
      val backend = storageBackendFactory(topic)

      withKafkaTopics(new NewTopic(topic, 1, 1.asInstanceOf[Short])) {
        withKafkaProducer { producer =>
          val messages = backend.generateRandomMessages(5, seed = 1)
          val loaderKafkaConfig = LoaderKafkaConfig(consumerGroup, topic)

          // Load messages
          loadMessagesThroughKafka(backend)(
            producer,
            loaderKafkaConfig,
            partition,
            messages,
            messageBatchCount = batchCount
          )

          // Confirm content
          eventually {
            backend.getContent.messages should contain theSameElementsAs Seq.fill(batchCount)(messages).flatten
          }
        }
      }
    }

    it(s"$testPrefix should restart from committed offset and continue writing from Kafka") {
      val testName = s"restart_test_$genTestId"
      val (topic, partition, consumerGroup) = (s"topic_$testName", 0, s"consumer_group_$testName")
      val backend = storageBackendFactory(topic)

      withKafkaTopics(new NewTopic(topic, 1, 1.asInstanceOf[Short])) {
        withKafkaProducer { producer =>
          val initialMessages = backend.generateRandomMessages(5, seed = 1)
          val loaderKafkaConfig = LoaderKafkaConfig(consumerGroup, topic)

          // Write some messages
          loadMessagesThroughKafka(backend)(
            producer,
            loaderKafkaConfig,
            partition,
            initialMessages,
            messageBatchCount = batchCount
          )

          // Write more messages
          val laterMessages = backend.generateRandomMessages(5, seed = 2)
          loadMessagesThroughKafka(backend)(
            producer,
            loaderKafkaConfig,
            partition,
            laterMessages,
            messageBatchCount = batchCount
          )

          // Check if everything got written
          eventually {
            val expected = Seq.fill(batchCount)(initialMessages ++ laterMessages).flatten
            backend.getContent.messages should contain theSameElementsAs expected
          }
        }
      }
    }
  }
}
