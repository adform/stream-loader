/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

import com.adform.streamloader.sink.file.FileCommitStrategy.FuzzyReachedAnyOf
import com.adform.streamloader.util.GaussianDistribution
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class FileCommitStrategyTest extends AnyFunSpec with Matchers {

  describe("FuzzyReachedAnyOf strategy") {
    val strategy = new FuzzyReachedAnyOf(
      fileOpenDurationDistribution = Some(GaussianDistribution(Duration.ofSeconds(10), Duration.ofSeconds(0))),
      fileSizeDistribution = Some(GaussianDistribution(1000, 0)),
      recordsWrittenDistribution = Some(GaussianDistribution(1000, 0))
    )()

    it("should commit after duration gets reached") {
      strategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      strategy.shouldCommit(Duration.ofSeconds(11), 1, 1) shouldEqual true
    }

    it("should commit after record count gets reached") {
      strategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      strategy.shouldCommit(Duration.ofSeconds(1), 1, 1001) shouldEqual true
    }

    it("should commit after file size gets reached") {
      strategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      strategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual true
    }

    it("should not commit if file size gets reached without reaching file size check sample size") {
      val sampledStrategy = strategy.copy(fileSizeSamplingBatchSize = Some(5))(None)

      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
    }

    it("should commit once file size reached and file size check sample size reached") {
      val sampledStrategy = strategy.copy(fileSizeSamplingBatchSize = Some(5))(None)

      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual true
    }

    it("should only invoke file size check after the check sample size reached") {
      val sampledStrategy = strategy.copy(fileSizeSamplingBatchSize = Some(5))(None)
      var invoked = 0

      def getFileSize: Long = {
        invoked += 1
        1001
      }

      sampledStrategy.shouldCommit(Duration.ofSeconds(1), getFileSize, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), getFileSize, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), getFileSize, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), getFileSize, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), getFileSize, 1) shouldEqual true

      invoked shouldEqual 1
    }

    it("should reset the file size sampling counter after commiting") {
      val sampledStrategy = strategy.copy(fileSizeSamplingBatchSize = Some(5))(None)

      // open duration gets reached first
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1001), 1, 1) shouldEqual true

      // now it should still take at least 5 records to trigger file size check
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual false
      sampledStrategy.shouldCommit(Duration.ofSeconds(1), 1001, 1) shouldEqual true
    }
  }
}
