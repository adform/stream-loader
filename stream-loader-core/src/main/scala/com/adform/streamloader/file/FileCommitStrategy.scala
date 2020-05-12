/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file

import java.time.Duration

import com.adform.streamloader.util.GaussianDistribution
import com.adform.streamloader.util.RandomExtensions._

import scala.util.Random

/**
  * A strategy for determining when files should be closed and committed to storage.
  */
trait FileCommitStrategy {

  /**
    * Checks whether a file should be closed and committed to storage.
    *
    * @param fileOpenDuration Time the file has been open for.
    * @param fileSize Current size of the file in bytes.
    * @param recordsWritten Number of records written to the file.
    * @return Whether the file should be closed and committed.
    */
  def shouldCommit(fileOpenDuration: Duration, fileSize: Long, recordsWritten: Long): Boolean
}

object FileCommitStrategy {

  /**
    * Commit strategy that commits files once ANY of the parameters reaches the given threshold.
    */
  case class ReachedAnyOf(
      fileOpenDuration: Option[Duration] = None,
      fileSize: Option[Long] = None,
      recordsWritten: Option[Long] = None
  ) extends FileCommitStrategy {

    require(
      fileOpenDuration.isDefined || fileSize.isDefined || recordsWritten.isDefined,
      "At least one upper limit for the file commit strategy has to be defined")

    override def shouldCommit(currFileOpenDuration: Duration, currFileSize: Long, currRecordsWritten: Long): Boolean = {
      fileOpenDuration.exists(d => currFileOpenDuration.toMillis >= d.toMillis) ||
      fileSize.exists(s => currFileSize >= s) ||
      recordsWritten.exists(r => currRecordsWritten >= r)
    }
  }

  /**
    * Commit strategy that commits files once ANY of the parameters reaches a threshold
    * value sampled from a given Gaussian distribution.
    */
  case class FuzzyReachedAnyOf(
      fileOpenDurationDistribution: Option[GaussianDistribution[Duration]] = None,
      fileSizeDistribution: Option[GaussianDistribution[Long]] = None,
      recordsWrittenDistribution: Option[GaussianDistribution[Long]] = None,
  )(randomSeed: Option[Int] = None)
      extends FileCommitStrategy {

    require(
      fileOpenDurationDistribution.isDefined || fileSizeDistribution.isDefined || recordsWrittenDistribution.isDefined,
      "At least one parameter distribution for the file commit strategy has to be defined"
    )

    private val rand = randomSeed.map(r => new Random(r)).getOrElse(new Random())

    private case class Parameters(
        fileOpenDuration: Option[Duration],
        fileSize: Option[Long],
        recordsWritten: Option[Long]
    )

    private def sampleParameters: Parameters = Parameters(
      fileOpenDurationDistribution.map(d => rand.nextGaussian(d)),
      fileSizeDistribution.map(d => rand.nextGaussian(d)),
      recordsWrittenDistribution.map(d => rand.nextGaussian(d))
    )

    private var currentSample = sampleParameters

    override def shouldCommit(currFileOpenDuration: Duration, currFileSize: Long, currRecordsWritten: Long): Boolean = {
      val shouldCommit = currentSample.fileOpenDuration.exists(d => currFileOpenDuration.toMillis >= d.toMillis) ||
        currentSample.fileSize.exists(s => currFileSize >= s) ||
        currentSample.recordsWritten.exists(r => currRecordsWritten >= r)

      if (shouldCommit) {
        // We only re-sample if we satisfy the conditions in the current sample, otherwise sampling is broken!
        currentSample = sampleParameters
      }
      shouldCommit
    }
  }
}
