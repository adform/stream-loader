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
import org.apache.kafka.clients.admin.{NewPartitions, NewTopic}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

trait RebalanceBehaviors { this: AnyFunSpec with Matchers with Eventually with Docker with Kafka with Loaders =>

  // The test is about kafka <-> loader integration, therefore storage backend shouldn't matter
  // We choose s3 for simplicity and speed. But it could be any one of them.

  private def genTestId: String = UUID.randomUUID().toString.take(6)

  def rebalancingLoader[M <: StorageMessage](testPrefix: String, storageBackendFactory: String => StorageBackend[M])(
      implicit ec: ExecutionContext
  ): Unit = {

    it(s"$testPrefix should load all messages from newly assigned partitions and continue loading old partitions") {

      val testName = s"rebalance_assign_test_$genTestId"
      val (topic, consumerGroup) = (s"topic_$testName", s"consumer_group_$testName")
      val backend = storageBackendFactory(topic)

      val initialTp = new TopicPartition(topic, 0)
      val additionalTp = new TopicPartition(topic, 1)

      withKafkaTopics(new NewTopic(topic, 1, 1.asInstanceOf[Short])) {
        withKafkaAdminClient { adminClient =>
          withKafkaProducer { producer =>
            // Test plan:
            // 1. Load some messages through a topic with one partition
            // 2. Create a new partition for that topic
            // 3. Send more messages into both partitions
            // 4. Confirm that both partitions were loaded

            // 1. Load some messages through a topic with one partition

            val messages = backend.generateRandomMessages(5, seed = 1)
            val secondPartitionMessages = backend.generateRandomMessages(5, seed = 2)
            val loaderKafkaConfig = LoaderKafkaConfig(consumerGroup, topic)

            // Load messages
            withLoader(backend)(
              loaderKafkaConfig,
              batchSize = messages.length
            ) { loader =>
              sendMessagesAndConfirm(backend)(
                producer,
                loaderKafkaConfig,
                initialTp,
                messages,
                messageBatchCount = 1
              )

              // Confirm content
              eventually {
                backend.getContent.messages should contain theSameElementsAs messages
              }

              // 2. Create a new partition for that topic

              val newPartitions: java.util.Map[String, NewPartitions] = new java.util.HashMap()
              newPartitions.put(topic, NewPartitions.increaseTo(2))
              adminClient.createPartitions(newPartitions)

              // 3. Send more messages into both partitions

              sendMessagesAndConfirm(backend)(
                producer,
                loaderKafkaConfig,
                initialTp,
                messages,
                messageBatchCount = 1
              )

              sendMessagesAndConfirm(backend)(
                producer,
                loaderKafkaConfig,
                additionalTp,
                secondPartitionMessages,
                messageBatchCount = 1
              )

              // 4. Confirm that both partitions were loaded
              eventually {
                backend.getContent.messages should contain theSameElementsAs (messages ++ messages ++ secondPartitionMessages)
              }
            }
          }
        }
      }
    }

    it(s"$testPrefix should stop loading messages from revoked partitions") {
      val testName = s"rebalance_revoke_test_$genTestId"
      val (topic, consumerGroup) = (s"topic_$testName", s"consumer_group_$testName")

      val backend = storageBackendFactory(topic)

      val firstTp = new TopicPartition(topic, 0)
      val secondTp = new TopicPartition(topic, 1)
      val tps = List(firstTp, secondTp)

      withKafkaTopics(new NewTopic(topic, tps.length, 1.asInstanceOf[Short])) {
        withKafkaAdminClient { adminClient =>
          withKafkaProducer { producer =>
            // Test plan:
            // 1. Load some messages through a topic with two partitions
            // 2. Create a second loader to take over one partition
            // 3. Confirm that the partitions eventually get rebalanced
            // 4. Send more messages to both partitions
            // 5. Confirm that everything gets loaded

            // 1. Load some messages through a topic with two partitions
            val messages = backend.generateRandomMessages(5, seed = 1)
            val loaderKafkaConfig = LoaderKafkaConfig(consumerGroup, topic)

            // Create loader
            withLoader(backend)(
              loaderKafkaConfig,
              batchSize = messages.length
            ) { _ =>
              // Send some messages
              tps.map(tp =>
                sendMessagesAndConfirm(backend)(
                  producer,
                  loaderKafkaConfig,
                  tp,
                  messages,
                  messageBatchCount = 1
                )
              )

              // Confirm messages loaded in the storage
              eventually {
                backend.getContent.messages should contain theSameElementsAs (messages ++ messages)
              }

              // 2. Create a second loader to take over one partition

              withLoader(backend)(
                loaderKafkaConfig,
                batchSize = messages.length
              ) { _ =>
                // 3. Confirm that the partitions eventually get rebalanced

                eventually(Timeout(Span(30, Seconds))) {
                  val cgDescription = adminClient
                    .describeConsumerGroups(Seq(consumerGroup).asJava)
                    .describedGroups()
                    .get(consumerGroup)
                    .get()
                  val memberAssignments =
                    cgDescription.members.asScala.map(md => (md.consumerId(), md.assignment().topicPartitions()))

                  // The two consumers in the group should have one partition each
                  memberAssignments.toList.map(_._2.asScala.toSet) should contain theSameElementsAs List(
                    Set(firstTp),
                    Set(secondTp)
                  )
                }

                // 4. Send more messages to both partitions

                val newMessages = backend.generateRandomMessages(5, seed = 2)

                List(firstTp, secondTp).map(tp =>
                  sendMessagesAndConfirm(backend)(
                    producer,
                    loaderKafkaConfig,
                    tp,
                    newMessages,
                    messageBatchCount = 1
                  )
                )

                // 5. Confirm that everything gets loaded
                eventually {
                  backend.getContent.messages shouldEqual (messages ++ messages ++ newMessages ++ newMessages)
                }
              }
            }
          }
        }
      }
    }
  }
}
