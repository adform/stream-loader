/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class RetryTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  class MockWorker(succeedAfterRuns: Int) {
    var runsUntilSuccess: Int = succeedAfterRuns
    var invocations: Int = 0

    def run(): Unit = {
      invocations += 1
      if (runsUntilSuccess > 0) {
        runsUntilSuccess -= 1
        throw new Exception()
      }
    }
  }

  describe("Retry policy") {

    it("should correctly adjust the policy after restarting") {
      Retry.Policy(2, 1.second, 3).minusRestart shouldEqual Retry.Policy(1, 3.seconds, 3)
    }

    it("should correctly specify if it should allow restarts") {
      Retry.Policy(0, 0.seconds, 3).shouldRetry shouldEqual false
      Retry.Policy(1, 0.seconds, 3).shouldRetry shouldEqual true
    }
  }

  it("should retry on failure") {
    val worker = new MockWorker(succeedAfterRuns = 1)
    Retry.retryOnFailure(Retry.Policy(1, 0.seconds, 1)) {
      worker.run()
    }
  }

  it("should not retry more times than requested") {
    val worker = new MockWorker(succeedAfterRuns = 5)
    assertThrows[Exception] {
      Retry.retryOnFailure(Retry.Policy(2, 0.seconds, 1)) {
        worker.run()
      }
    }
    worker.invocations shouldEqual 3
  }

  it("should retry until the specified condition is met") {
    var cnt = 5
    var invocations = 0
    Retry.retryUntil(Retry.Policy(10, 0.seconds, 1)) { _: Unit =>
      cnt -= 1; cnt == 0
    } {
      invocations += 1
    }
    invocations shouldEqual 5
  }

  it("should retry only while a required condition is met") {
    val worker = new MockWorker(succeedAfterRuns = 100)
    var cnt = 5
    Retry.retryOnFailureIf(Retry.Policy(10, 0.seconds, 1)) {
      cnt -= 1; cnt > 0
    } {
      worker.run()
    }
    worker.invocations shouldEqual 5
  }
}
