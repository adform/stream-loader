/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.s3

import com.adform.streamloader.batch.storage.TwoPhaseCommitBatchStorage
import com.adform.streamloader.file.{FilePathFormatter, FileRecordBatch, PartitionedFileRecordBatch}
import com.adform.streamloader.util.Logging
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters._

/**
  * An S3 compatible file storage, stores files and commits offsets to Kafka in a two-phase transaction.
  * The file upload prepare/stage phases consist of starting and completing a multi-part upload with a single part.
  */
class S3FileStorage[P](
    s3Client: S3Client,
    bucket: String,
    filePathFormatter: FilePathFormatter[P]
) extends TwoPhaseCommitBatchStorage[PartitionedFileRecordBatch[P, FileRecordBatch], S3MultiFileStaging]
    with Logging {

  override protected def stageBatch(batch: PartitionedFileRecordBatch[P, FileRecordBatch]): S3MultiFileStaging = {
    val stagings = batch.partitionBatches.map { case (partition, partitionBatch) =>
      stageSingleBatch(partition, partitionBatch)
    }
    log.debug(s"Successfully staged batch $batch")
    S3MultiFileStaging(stagings.toSeq)
  }

  override protected def storeBatch(staging: S3MultiFileStaging): Unit = {
    staging.fileUploads.foreach(fs => storeSingleBatch(fs))
    log.info(s"Successfully stored batch $staging")
  }

  override protected def isBatchStored(staging: S3MultiFileStaging): Boolean = {
    staging.fileUploads.forall(s => listObjects(s.destinationKey).nonEmpty)
  }

  private def listObjects(prefix: String): Seq[S3Object] = {
    val request = ListObjectsV2Request
      .builder()
      .maxKeys(Int.MaxValue)
      .bucket(bucket)
      .prefix(prefix)
      .build()

    val iterable = s3Client.listObjectsV2Paginator(request)

    val objects = List.newBuilder[S3Object]
    iterable.stream.forEach(p => objects ++= p.contents.asScala)

    objects.result()
  }

  private def stageSingleBatch(partition: P, batch: FileRecordBatch): S3FileStaging = {
    val path = filePathFormatter.formatPath(partition, batch.recordRanges)
    val uploadRequest = CreateMultipartUploadRequest.builder().bucket(bucket).key(path).build()

    val upload = s3Client.createMultipartUpload(uploadRequest)
    val uploadId = upload.uploadId()

    val uploadPartRequest = UploadPartRequest
      .builder()
      .bucket(bucket)
      .key(path)
      .uploadId(uploadId)
      .partNumber(1)
      .build()

    log.debug(s"Starting multi-part upload with ID $uploadId for file $path")

    val uploadPartResult = s3Client.uploadPart(uploadPartRequest, RequestBody.fromFile(batch.file))
    val uploadedPartTag = uploadPartResult.eTag()

    log.debug(s"Staged file to multi-part upload ID $uploadId with tag $uploadedPartTag for file $path")
    S3FileStaging(uploadId, uploadedPartTag, path)
  }

  private def storeSingleBatch(staging: S3FileStaging): Unit = {
    val (tag, uploadId) = (staging.uploadPartTag, staging.uploadId)
    val completePartRequest = CompletedPart
      .builder()
      .partNumber(1)
      .eTag(tag)
      .build()

    log.debug(s"Completing multi-part upload ID $uploadId with tag $tag")

    val completedUploadRequest = CompletedMultipartUpload.builder().parts(completePartRequest).build()
    val completeRequest =
      CompleteMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(staging.destinationKey)
        .uploadId(uploadId)
        .multipartUpload(completedUploadRequest)
        .build()

    s3Client.completeMultipartUpload(completeRequest)
    log.debug(s"Completed multi-part upload ID $uploadId with tag $tag to file ${staging.destinationKey}")
  }
}

object S3FileStorage {
  case class Builder[P](
      private val _s3Client: S3Client,
      private val _bucket: String,
      private val _filePathFormatter: FilePathFormatter[P]
  ) {

    /**
      * Sets the S3 client to use.
      */
    def s3Client(client: S3Client): Builder[P] = copy(_s3Client = client)

    /**
      * Sets the bucket to upload to.
      */
    def bucket(name: String): Builder[P] = copy(_bucket = name)

    /**
      * Sets the formatter for forming file paths, i.e. keys in S3.
      */
    def filePathFormatter(formatter: FilePathFormatter[P]): Builder[P] = copy(_filePathFormatter = formatter)

    def build(): S3FileStorage[P] = {
      if (_s3Client == null) throw new IllegalArgumentException("Must provide an S3 client")
      if (_bucket == null) throw new IllegalArgumentException("Must provide a valid bucket")
      if (_filePathFormatter == null) throw new IllegalArgumentException("Must provide a file path formatter")

      new S3FileStorage(_s3Client, _bucket, _filePathFormatter)
    }
  }

  def builder[P](): Builder[P] = Builder[P](null, null, null)
}
