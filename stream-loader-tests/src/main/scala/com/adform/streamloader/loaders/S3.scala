/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import java.net.URI

import com.adform.streamloader.encoding.csv.CsvFileBuilderFactory
import com.adform.streamloader.file._
import com.adform.streamloader.s3.S3FileStorage
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.{KafkaSource, Loader, StreamLoader}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.TopicPartition
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

class BaseS3Loader extends Loader {

  def groupForPartition(tp: TopicPartition): String = "root"

  def main(args: Array[String]): Unit = {

    val cfg = ConfigFactory.load().getConfig("stream-loader")

    val s3Client: S3Client = {
      val builder = S3Client
        .builder()
        .region(cfg.getStringOpt("s3.region").map(Region.of).getOrElse(Region.EU_WEST_1))
        .credentialsProvider(
          () =>
            AwsBasicCredentials.create(
              cfg.getString("s3.access-key"),
              cfg.getString("s3.secret-key")
          ))
      cfg.getStringOpt("s3.endpoint").foreach(endpoint => builder.endpointOverride(new URI(endpoint)))
      builder.build()
    }

    val recordFormatter: RecordFormatter[String] = record => {
      val tp = s"${record.consumerRecord.topic()};${record.consumerRecord.partition()}"
      val offset = record.consumerRecord.offset().toString
      val watermark = record.watermark.millis.toString
      val msg = new String(record.consumerRecord.value, "UTF-8")
      Seq(s"$tp;$offset;$watermark;$msg")
    }

    val source = KafkaSource
      .builder()
      .consumerProperties(cfg.getConfig("kafka.consumer").toProperties)
      .pollTimeout(cfg.getDuration("kafka.poll-timeout"))
      .topics(Seq(cfg.getString("kafka.topic")))
      .build()

    val sink =
      FileSink
        .builder()
        .fileStorage(
          S3FileStorage
            .builder()
            .s3Client(s3Client)
            .bucket(cfg.getString("s3.bucket"))
            .filePathFormatter(
              new TimePartitioningFilePathFormatter(cfg.getStringOpt("file.time-partition.pattern"), None)
            )
            .build()
        )
        .fileCommitStrategy(
          FileCommitStrategy.ReachedAnyOf(recordsWritten = Some(cfg.getLong("file.max.records")))
        )
        .partitionGrouping(groupForPartition)
        .fileBuilderFactory(new CsvFileBuilderFactory[String](Compression.NONE))
        .recordFormatter(recordFormatter)
        .build()

    val loader = new StreamLoader(source, sink)

    sys.addShutdownHook {
      loader.stop()
      s3Client.close()
    }

    loader.start()
  }
}

object TestS3Loader extends BaseS3Loader

object TestGroupingS3Loader extends BaseS3Loader {
  override def groupForPartition(tp: TopicPartition): String = s"partition_${tp.partition % 2}"
}
