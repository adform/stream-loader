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
    * Checks whether the given exception is caused by an interrupt.
    */
  def isInterruptionException(e: Throwable): Boolean = e match {
    case _: InterruptedException | _: InterruptedIOException | _: ClosedByInterruptException | _: InterruptException =>
      true
    case e: Throwable if e.getCause != null && isInterruptionException(e.getCause) => true
    case _ => false
  }

  /**
    * Executes the given `code`, retrying as many times as needed if it throws non-interrupt exceptions.
    * Returns the result of the last successful execution or throws an exception if either it gets interrupted or is out of retries.
    */
  @tailrec def retryOnFailure[R](delay: Duration = 1.second, backoffFactor: Int = 3, retries: Int = Int.MaxValue)(
      code: => R): R =
    Try(code) match {
      case Success(result) => result
      case Failure(e) if NonFatal(e) && !isInterruptionException(e) =>
        if (retries > 0) {
          log.warn(e)(s"Encountered failure, retrying in ${delay.toSeconds} seconds")
          Thread.sleep(delay.toMillis)
          retryOnFailure(delay * backoffFactor, backoffFactor, retries - 1)(code)
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
  @tailrec def retryOnFailureIf(delay: Duration = 1.second, backoffFactor: Int = 3, retries: Int = Int.MaxValue)(
      shouldRetry: => Boolean)(code: => Unit): Unit =
    Try(code) match {
      case Success(_) => ()
      case Failure(e) if NonFatal(e) && !isInterruptionException(e) =>
        if (retries > 0) {
          log.warn(e)(s"Encountered failure, checking if a retry is needed")
          if (shouldRetry) {
            log.warn(s"Retry needed, retrying in ${delay.toSeconds} seconds")
            Thread.sleep(delay.toMillis)
            retryOnFailureIf(delay * backoffFactor, backoffFactor, retries - 1)(shouldRetry)(code)
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
  @tailrec def retryUntil[R](delay: Duration = 1.second, backoffFactor: Int = 3, retries: Int = Int.MaxValue)(
      check: R => Boolean)(code: => R): R = {
    val result = code
    if (!check(result) && retries > 0) {
      Thread.sleep(delay.toMillis)
      retryUntil(delay * backoffFactor, backoffFactor, retries - 1)(check)(code)
    } else {
      result
    }
  }
}
