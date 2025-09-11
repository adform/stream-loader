/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.io.InterruptedIOException
import java.nio.channels.ClosedByInterruptException

import org.apache.kafka.common.errors.InterruptException

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Retry extends Logging {

  /**
    * Description of a retry policy.
    *
    * @param retriesLeft Number of retries left.
    * @param initialDelay Initial delay to use upon the next restart.
    * @param backoffFactor Factor to increase the delay by after every restart.
    */
  case class Policy(retriesLeft: Int, initialDelay: Duration, backoffFactor: Int) {

    /**
      * Returns a copy of the current policy minus one restart.
      */
    def minusRestart: Policy = copy(retriesLeft - 1, initialDelay * backoffFactor, backoffFactor)

    /**
      * Checks whether the current policy has retries left.
      */
    def shouldRetry: Boolean = retriesLeft > 0
  }

  /**
    * Checks whether the given exception is caused by an interrupt.
    */
  def isInterruptionException(e: Throwable): Boolean = e match {
    case _: InterruptedException | _: InterruptedIOException | _: ClosedByInterruptException | _: InterruptException =>
      true
    case _ => false
  }

  /**
    * Executes the given `code`, retrying as many times as needed if it throws non-interrupt exceptions.
    * Returns the result of the last successful execution or throws an exception if either it gets interrupted or is out of retries.
    */
  @tailrec def retryOnFailure[R](retryPolicy: Policy)(code: => R): R =
    Try(code) match {
      case Success(result) => result
      case Failure(e) if NonFatal(e) && !isInterruptionException(e) =>
        if (retryPolicy.shouldRetry) {
          log.warn(e)(s"Encountered failure, retrying in ${retryPolicy.initialDelay.toSeconds} seconds")
          Thread.sleep(retryPolicy.initialDelay.toMillis)
          retryOnFailure(retryPolicy.minusRestart)(code)
        } else {
          log.error(e)("Encountered failure and ran out of retries, failing hard")
          throw e
        }
      case Failure(e) =>
        throw e
    }

  /**
    * Executes the given `code`, retrying as many times as needed if it throws non-interrupt exceptions and if the given
    * `shouldRetry` predicate evaluates to true.
    */
  @tailrec def retryOnFailureIf(policy: Policy)(shouldRetry: => Boolean)(code: => Unit): Unit =
    Try(code) match {
      case Success(_) => ()
      case Failure(e) if NonFatal(e) && !isInterruptionException(e) =>
        if (policy.shouldRetry) {
          log.warn(e)(s"Encountered failure, checking if a retry is needed")
          if (shouldRetry) {
            log.warn(s"Retry needed, retrying in ${policy.initialDelay.toSeconds} seconds")
            Thread.sleep(policy.initialDelay.toMillis)
            retryOnFailureIf(policy.minusRestart)(shouldRetry)(code)
          } else {
            log.info("Retry not needed, proceeding")
          }
        } else {
          log.error(e)("Encountered failure and ran out of retries, failing hard")
          throw e
        }
      case Failure(e) =>
        throw e
    }

  /**
    * Executes the given `code` until the specified `check` returns true.
    * Returns the result of the last execution, even if we run out of retries.
    */
  @tailrec def retryUntil[R](policy: Policy)(check: R => Boolean)(code: => R): R = {
    val result = code
    if (!check(result) && policy.shouldRetry) {
      Thread.sleep(policy.initialDelay.toMillis)
      retryUntil(policy.minusRestart)(check)(code)
    } else {
      result
    }
  }
}
