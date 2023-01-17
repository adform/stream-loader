/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch.storage

import com.adform.streamloader.sink.file.SingleFileRecordBatch

import scala.collection.mutable

class MockTwoPhaseCommitFileStorage extends TwoPhaseCommitBatchStorage[SingleFileRecordBatch, FileStaging] {

  val stagedFiles = mutable.Set.empty[String]
  val storedFiles = mutable.Set.empty[String]

  private var stagesInvoked = 0
  private var failOnStage = -1

  def failOnFileStagingOnce(seqNo: Int = 1): Unit = {
    stagesInvoked = 0
    failOnStage = seqNo
  }

  override def stageBatch(batch: SingleFileRecordBatch): FileStaging = {
    stagesInvoked += 1
    if (stagesInvoked == failOnStage) {
      throw new UnsupportedOperationException("File staging failed")
    }
    val path = batch.file.getAbsolutePath
    val staging = FileStaging(path + ".part", path)

    stagedFiles.add(staging.stagingPath)
    staging
  }

  private var storesInvoked = 0
  private var failOnStore = -1

  def failOnFileStoreOnce(seqNo: Int = 1): Unit = {
    storesInvoked = 0
    failOnStore = seqNo
  }

  override def storeBatch(fileStaging: FileStaging): Unit = {
    storesInvoked += 1
    if (storesInvoked == failOnStore) {
      throw new UnsupportedOperationException("File storing failed")
    }
    stagedFiles.remove(fileStaging.stagingPath)
    storedFiles.add(fileStaging.destinationPath)
  }

  override def isBatchStored(fileStaging: FileStaging): Boolean = storedFiles.contains(fileStaging.destinationPath)
}
