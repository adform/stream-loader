/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.model.Timestamp
import com.adform.streamloader.util.{Logging, TimeProvider}

import java.time.Duration

/**
  * Trait for implementing watermark tracking in a stream.
  */
trait WatermarkProvider {

  /**
    * Initializes the provider to a specified watermark.
    *
    * @param initialWatermark The initial watermark to initialize to.
    * @return The initial watermark.
    */
  def initialize(initialWatermark: Timestamp): Timestamp

  /**
    * Observes a new event and moves the watermark forward if needed.
    *
    * @param timestamp Timestamp of the new event.
    * @return The current watermark.
    */
  def observeEvent(timestamp: Timestamp): Timestamp

  /**
    * Returns the current watermark.
    */
  def currentWatermark: Timestamp
}

/**
  * Watermark provider that sets the watermark to the maximum observed event time.
  * In order to protect from malformed messages with timestamps from the future, values greater than a predefined
  * threshold above the current time are rejected and do not advance the watermark.
  *
  * @param validWatermarkDiff Upper limit for setting watermarks greater than the current time.
  */
class MaxWatermarkProvider(validWatermarkDiff: Duration)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends WatermarkProvider
    with Logging {

  private val validWatermarkDiffMillis = validWatermarkDiff.toMillis
  private var watermark = Timestamp(-1L)

  def initialize(initialWatermark: Timestamp): Timestamp = {
    watermark = initialWatermark
    watermark
  }

  def observeEvent(timestamp: Timestamp): Timestamp = {
    if (timestamp.millis <= timeProvider.currentMillis + validWatermarkDiffMillis) {
      if (timestamp > watermark)
        watermark = timestamp
    } else {
      log.warn(
        s"Received a message with an out of bounds timestamp $timestamp (" + timestamp
          .format("yyyy/MM/dd HH:mm:ss")
          .get + "), ignoring and not advancing the watermark")
    }
    watermark
  }

  def currentWatermark: Timestamp = watermark
}
