/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import java.io.File
import java.nio.file.{Files, Paths}

import com.adform.streamloader.file.{FilePathFormatter, RecordRangeFile}
import com.adform.streamloader.model.RecordRange

/**
  * A local file storage implementation.
  */
class LocalFileStorage(
    stagingBasePath: File,
    stagingFilePathFormatter: FilePathFormatter,
    destinationBasePath: File,
    destinationFilePathFormatter: FilePathFormatter,
) extends TwoPhaseCommitFileStorage[Unit] {

  override def startNewFile(): Unit = {}

  override protected def stageFile(file: RecordRangeFile[Unit]): FileStaging = {
    val stagingPath = new File(stagingBasePath, stagingFilePathFormatter.formatPath(file.recordRanges))
    val destinationPath = new File(destinationBasePath, destinationFilePathFormatter.formatPath(file.recordRanges))

    log.info(s"Moving file ${file.file.getAbsolutePath} to staging path $stagingPath")
    stagingPath.getParentFile.mkdirs()
    Files.move(Paths.get(file.file.toURI), Paths.get(stagingPath.toURI))

    FileStaging(stagingPath.getAbsolutePath, destinationPath.getAbsolutePath)
  }

  override protected def storeFile(fileStaging: FileStaging): Unit = {
    log.info(s"Moving staged file ${fileStaging.stagingPath} to destination path ${fileStaging.destinationPath}")
    new File(fileStaging.destinationPath).getParentFile.mkdirs()
    Files.move(Paths.get(fileStaging.stagingPath), Paths.get(fileStaging.destinationPath))
  }

  override protected def isFileStored(fileStaging: FileStaging): Boolean = {
    Files.exists(Paths.get(fileStaging.destinationPath))
  }
}

object LocalFileStorage {

  case class Builder(
      private val _stagingBasePath: File,
      private val _stagingFilePathFormatter: FilePathFormatter,
      private val _destinationBasePath: File,
      private val _destinationFilePathFormatter: FilePathFormatter
  ) {

    def stagingBasePath(path: File): Builder = copy(_stagingBasePath = path)
    def stagingBasePath(path: String): Builder = copy(_stagingBasePath = new File(path))
    def destinationBasePath(path: File): Builder = copy(_destinationBasePath = path)
    def destinationBasePath(path: String): Builder = copy(_destinationBasePath = new File(path))
    def stagingFilePathFormatter(formatter: FilePathFormatter): Builder = copy(_stagingFilePathFormatter = formatter)
    def destinationFilePathFormatter(formatter: FilePathFormatter): Builder =
      copy(_destinationFilePathFormatter = formatter)

    def build(): FileStorage[Unit] = {
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

      new LocalFileStorage(_stagingBasePath, stagingFormatter, _destinationBasePath, _destinationFilePathFormatter)
    }
  }

  def builder(): Builder = Builder(null, null, null, null)
}
