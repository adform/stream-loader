/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop

import java.io.IOException

import com.adform.streamloader.file.storage.{FileStaging, FileStorage, TwoPhaseCommitFileStorage}
import com.adform.streamloader.file.{FilePathFormatter, RecordRangeFile}
import com.adform.streamloader.model.RecordRange
import com.adform.streamloader.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * A Hadoop compatible file system based storage, most likely used for storing to HDFS.
  * Stores files and commits offsets to Kafka in a two-phase transaction.
  * The prepare/commit phases for storing a file consist of first uploading it to a staging path
  * and later atomically moving it to the final destination path.
  */
class HadoopFileStorage private (
    hadoopFS: FileSystem,
    stagingDirectory: String,
    stagingFilePathFormatter: FilePathFormatter,
    destinationDirectory: String,
    destinationFilePathFormatter: FilePathFormatter
) extends TwoPhaseCommitFileStorage[Unit]
    with Logging {

  private val stagingPath = new Path(stagingDirectory)
  private val basePath = new Path(destinationDirectory)

  override def startNewFile(): Unit = {}

  override protected def stageFile(file: RecordRangeFile[Unit]): FileStaging = {
    val sourceFilePath = new Path(file.file.toPath.toString)
    val stagingFilePath = new Path(stagingPath, stagingFilePathFormatter.formatPath(file.recordRanges))
    val targetFilePath = new Path(basePath, destinationFilePathFormatter.formatPath(file.recordRanges))

    log.debug(s"Staging file $sourceFilePath to $stagingFilePath")

    hadoopFS.copyFromLocalFile(false, true, sourceFilePath, stagingFilePath)

    FileStaging(stagingFilePath.toUri.toString, targetFilePath.toUri.toString)
  }

  override protected def storeFile(fileStaging: FileStaging): Unit = {
    val stagingFilePath = new Path(fileStaging.stagingPath)
    val targetFilePath = new Path(fileStaging.destinationPath)

    if (!hadoopFS.exists(targetFilePath.getParent)) {
      log.debug(s"Creating directory ${targetFilePath.getParent}")
      hadoopFS.mkdirs(targetFilePath.getParent)
    }

    log.debug(s"Moving staged file $stagingFilePath to the destination path $targetFilePath")
    if (!hadoopFS.rename(stagingFilePath, targetFilePath)) {
      if (!isFileStored(fileStaging)) {
        throw new IOException(
          s"Failed renaming file from $stagingFilePath to $targetFilePath, because $stagingFilePath does not exist")
      } else {
        throw new IOException(s"Failed renaming file from $stagingFilePath to $targetFilePath")
      }
    }
    log.info(s"Successfully stored staged file $stagingFilePath to the destination path $targetFilePath")
  }

  override protected def isFileStored(fileStaging: FileStaging): Boolean = {
    hadoopFS.exists(new Path(fileStaging.destinationPath))
  }
}

object HadoopFileStorage {

  case class Builder(
      private val _hadoopFS: FileSystem,
      private val _stagingBasePath: String,
      private val _stagingFilePathFormatter: FilePathFormatter,
      private val _destinationBasePath: String,
      private val _destinationFilePathFormatter: FilePathFormatter
  ) {

    /**
      * Sets the Hadoop file system to use.
      */
    def hadoopFS(fs: FileSystem): Builder = copy(_hadoopFS = fs)

    /**
      * Sets the staging base path (directory) in the file system.
      */
    def stagingBasePath(path: String): Builder = copy(_stagingBasePath = path)

    /**
      * Sets the destination base path (directory) in the file system.
      */
    def destinationBasePath(path: String): Builder = copy(_destinationBasePath = path)

    /**
      * Sets the file path formatter for the staged files.
      * If not provided, the destination formatter is used with an additional ".tmp" suffix appended.
      */
    def stagingFilePathFormatter(formatter: FilePathFormatter): Builder = copy(_stagingFilePathFormatter = formatter)

    /**
      * Sets the file path formatter for the destination files.
      * Can include a prefix directories for partitioning.
      */
    def destinationFilePathFormatter(formatter: FilePathFormatter): Builder =
      copy(_destinationFilePathFormatter = formatter)

    def build(): FileStorage[Unit] = {
      if (_hadoopFS == null) throw new IllegalArgumentException("Must provide a Hadoop FileSystem")
      if (_stagingBasePath == null) throw new IllegalArgumentException("Staging base path must be provided")
      if (_destinationBasePath == null) throw new IllegalArgumentException("Destination base path must be provided")
      if (_destinationFilePathFormatter == null)
        throw new IllegalArgumentException("Destination file path formatter must be provided")

      val stagingFormatter = if (_stagingFilePathFormatter != null) {
        _stagingFilePathFormatter
      } else {
        new FilePathFormatter {
          override def formatPath(ranges: Seq[RecordRange]): String =
            _destinationFilePathFormatter.formatPath(ranges) + ".tmp"
        }
      }

      new HadoopFileStorage(
        _hadoopFS,
        _stagingBasePath,
        stagingFormatter,
        _destinationBasePath,
        _destinationFilePathFormatter)
    }
  }

  def builder(): Builder = Builder(null, null, null, null, null)
}
