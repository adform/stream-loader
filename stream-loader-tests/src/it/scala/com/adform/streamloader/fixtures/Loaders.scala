/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.fixtures

import java.util.concurrent.TimeUnit

import com.adform.streamloader.model.StorageMessage
import com.adform.streamloader.storage.{LoaderKafkaConfig, StorageBackend}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.log4s.{Logger, getLogger}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait Loaders { this: Docker =>

  private val log: Logger = getLogger(getClass)

  type ProducedRecord = ProducerRecord[Array[Byte], Array[Byte]]

  private def sendMessage(kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]])(
      tp: TopicPartition,
      key: String,
      value: Array[Byte])(implicit ec: ExecutionContext): Future[(ProducedRecord, RecordMetadata)] = {

    val record = new ProducerRecord[Array[Byte], Array[Byte]](
      tp.topic(),
      tp.partition(),
      key.getBytes(),
      value
    )

    toScalaFuture(kafkaProducer.send(record)).map(result => (record, result))
  }

  def withLoader[T, A <: StorageMessage](backend: StorageBackend[A])(kafkaConfig: LoaderKafkaConfig, batchSize: Int)(
      code: Container => T)(implicit ec: ExecutionContext): T = {
    val loader = backend.createLoaderContainer(kafkaConfig, batchSize)

    withContainer(loader)(code(loader))
  }

  def sendMessagesAndConfirm[A <: StorageMessage](backend: StorageBackend[A])(
      producer: KafkaProducer[Array[Byte], Array[Byte]],
      loaderKafkaConfig: LoaderKafkaConfig,
      tp: TopicPartition,
      messageBatch: Seq[A],
      messageBatchCount: Int)(implicit ec: ExecutionContext): Boolean = {

    val messageSendResults = for {
      batch <- 0 until messageBatchCount
      (message, idx) <- messageBatch.zipWithIndex
      n = (messageBatch.size * batch) + idx
    } yield {
      sendMessage(producer)(tp, n.toString, message.getBytes)
    }

    val maxSentOffsetFuture = Future.sequence(messageSendResults).map(messages => messages.map(_._2.offset()).max)
    val maxSentOffset = Await.result(maxSentOffsetFuture, Duration(60, TimeUnit.SECONDS))
    def maxStoredOffset: Option[Long] = backend.committedPositions(loaderKafkaConfig, Set(tp))(tp).map(_.offset)

    var remainingIterations = 240
    var succeeded = false

    while (!succeeded && remainingIterations > 0) {
      val currentStoredOffset = maxStoredOffset
      succeeded = currentStoredOffset.contains(maxSentOffset + 1)
      if (!succeeded) {
        log.debug(s"Waiting for $maxSentOffset, current stored offset is $currentStoredOffset")
        Thread.sleep(500)
        remainingIterations -= 1
      }
    }
    if (!succeeded) {
      log.warn(s"Timed out while waiting for offsets to be committed to storage")
    }
    succeeded
  }

  def loadMessagesThroughKafka[A <: StorageMessage](backend: StorageBackend[A])(
      producer: KafkaProducer[Array[Byte], Array[Byte]],
      loaderKafkaConfig: LoaderKafkaConfig,
      partition: Int,
      messageBatch: Seq[A],
      messageBatchCount: Int
  )(implicit ec: ExecutionContext): Boolean = {
    withLoader(backend)(loaderKafkaConfig, messageBatch.length) { _ =>
      val tp = new TopicPartition(loaderKafkaConfig.topic, partition)
      sendMessagesAndConfirm(backend)(producer, loaderKafkaConfig, tp, messageBatch, messageBatchCount)
    }
  }

  private def toScalaFuture[T](javaFuture: java.util.concurrent.Future[T])(implicit ec: ExecutionContext): Future[T] =
    Future {
      while (!javaFuture.isDone) Thread.sleep(10)
      javaFuture.get()
    }
}
