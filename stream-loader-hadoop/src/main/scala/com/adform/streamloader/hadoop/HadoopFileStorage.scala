/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop

import com.adform.streamloader.batch.storage.TwoPhaseCommitBatchStorage
import com.adform.streamloader.file.{BaseFileRecordBatch, FilePathFormatter, PartitionedFileRecordBatch}
import com.adform.streamloader.model.RecordRange
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.IOException

/**
  * A Hadoop compatible file system based storage, most likely used for storing to HDFS.
  * Stores files and commits offsets to Kafka in a two-phase transaction.
  * The prepare/commit phases for storing a file consist of first uploading it to a staging path
  * and later atomically moving it to the final destination path.
  */
class HadoopFileStorage[P](
    hadoopFS: FileSystem,
    stagingDirectory: String,
    stagingFilePathFormatter: FilePathFormatter[P],
    destinationDirectory: String,
    destinationFilePathFormatter: FilePathFormatter[P]
) extends TwoPhaseCommitBatchStorage[PartitionedFileRecordBatch[P, BaseFileRecordBatch], MultiFileStaging] {

  private val stagingPath = new Path(stagingDirectory)
  private val basePath = new Path(destinationDirectory)


  override protected def stageBatch(batch: PartitionedFileRecordBatch[P, BaseFileRecordBatch]): MultiFileStaging = {
    val stagings = batch.partitionBatches.map {
      case (partition, fileBatch) => stageSingleBatch(partition, fileBatch)
    }
    log.debug(s"Successfully staged batch $batch")
    MultiFileStaging(stagings.toSeq)
  }

  override protected def storeBatch(staging: MultiFileStaging): Unit = {
    staging.fileStagings.foreach(fs => storeSingleBatch(fs))
    log.info(s"Successfully stored staged batch $staging")
  }

  override protected def isBatchStored(staging: MultiFileStaging): Boolean = {
    staging.fileStagings.forall(fs => isSingleBatchStored(fs))
  }

  private def stageSingleBatch(partition: P, batch: BaseFileRecordBatch): FileStaging = {
    val sourceFilePath = new Path(batch.file.toPath.toString)
    val stagingFilePath = new Path(stagingPath, stagingFilePathFormatter.formatPath(partition, batch.recordRanges))
    val targetFilePath = new Path(basePath, destinationFilePathFormatter.formatPath(partition, batch.recordRanges))

    log.debug(s"Staging file $sourceFilePath to $stagingFilePath")

    hadoopFS.copyFromLocalFile(false, true, sourceFilePath, stagingFilePath)

    FileStaging(stagingFilePath.toUri.toString, targetFilePath.toUri.toString)
  }

  private def storeSingleBatch(staging: FileStaging): Unit = {
    val stagingFilePath = new Path(staging.stagingPath)
    val targetFilePath = new Path(staging.destinationPath)

    if (!hadoopFS.exists(targetFilePath.getParent)) {
      log.debug(s"Creating directory ${targetFilePath.getParent}")
      hadoopFS.mkdirs(targetFilePath.getParent)
    }

    log.debug(s"Moving staged file $stagingFilePath to the destination path $targetFilePath")
    if (!hadoopFS.rename(stagingFilePath, targetFilePath)) {
      if (!isSingleBatchStored(staging)) {
        throw new IOException(
          s"Failed renaming file from $stagingFilePath to $targetFilePath, because $stagingFilePath does not exist")
      } else {
        throw new IOException(s"Failed renaming file from $stagingFilePath to $targetFilePath")
      }
    }
    log.debug(s"Successfully stored staged file $stagingFilePath to the destination path $targetFilePath")
  }

  private def isSingleBatchStored(staging: FileStaging): Boolean = {
    hadoopFS.exists(new Path(staging.destinationPath))
  }
}

object HadoopFileStorage {

  case class Builder[P](
      private val _hadoopFS: FileSystem,
      private val _stagingBasePath: String,
      private val _stagingFilePathFormatter: FilePathFormatter[P],
      private val _destinationBasePath: String,
      private val _destinationFilePathFormatter: FilePathFormatter[P]
  ) {

    /**
      * Sets the Hadoop file system to use.
      */
    def hadoopFS(fs: FileSystem): Builder[P] = copy(_hadoopFS = fs)

    /**
      * Sets the staging base path (directory) in the file system.
      */
    def stagingBasePath(path: String): Builder[P] = copy(_stagingBasePath = path)

    /**
      * Sets the destination base path (directory) in the file system.
      */
    def destinationBasePath(path: String): Builder[P] = copy(_destinationBasePath = path)

    /**
      * Sets the file path formatter for the staged files.
      * If not provided, the destination formatter is used with an additional ".tmp" suffix appended.
      */
    def stagingFilePathFormatter(formatter: FilePathFormatter[P]): Builder[P] =
      copy(_stagingFilePathFormatter = formatter)

    /**
      * Sets the file path formatter for the destination files.
      * Can include a prefix directories for partitioning.
      */
    def destinationFilePathFormatter(formatter: FilePathFormatter[P]): Builder[P] =
      copy(_destinationFilePathFormatter = formatter)

    def build(): HadoopFileStorage[P] = {
      if (_hadoopFS == null) throw new IllegalArgumentException("Must provide a Hadoop FileSystem")
      if (_stagingBasePath == null) throw new IllegalArgumentException("Staging base path must be provided")
      if (_destinationBasePath == null) throw new IllegalArgumentException("Destination base path must be provided")
      if (_destinationFilePathFormatter == null)
        throw new IllegalArgumentException("Destination file path formatter must be provided")

      val stagingFormatter = if (_stagingFilePathFormatter != null) {
        _stagingFilePathFormatter
      } else {
        new FilePathFormatter[P] {
          override def formatPath(partition: P, ranges: Seq[RecordRange]): String =
            _destinationFilePathFormatter.formatPath(partition, ranges) + ".tmp"
        }
      }

      new HadoopFileStorage[P](
        _hadoopFS,
        _stagingBasePath,
        stagingFormatter,
        _destinationBasePath,
        _destinationFilePathFormatter)
    }
  }

  def builder[P](): Builder[P] = Builder[P](null, null, null, null, null)
}
