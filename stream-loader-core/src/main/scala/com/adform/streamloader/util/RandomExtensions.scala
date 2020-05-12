/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.time.Duration

import scala.util.Random

case class GaussianDistribution[T](mean: T, stdDev: T)

object RandomExtensions {

  /**
    * A vector space structure on a type `T`, implementers need to define addition and scalar multiplication.
    */
  trait VectorSpace[T] {
    def add(a: T, b: T): T
    def multiply(c: Double, a: T): T
  }

  implicit val IntVectorSpace: VectorSpace[Int] = new VectorSpace[Int] {
    override def add(a: Int, b: Int): Int = a + b
    override def multiply(c: Double, a: Int): Int = (c * a).toInt
  }

  implicit val LongVectorSpace: VectorSpace[Long] = new VectorSpace[Long] {
    override def add(a: Long, b: Long): Long = a + b
    override def multiply(c: Double, a: Long): Long = (c * a).toLong
  }

  implicit val DurationVectorSpace: VectorSpace[Duration] = new VectorSpace[Duration] {
    override def add(a: Duration, b: Duration): Duration = Duration.ofMillis(a.toMillis + b.toMillis)
    override def multiply(c: Double, a: Duration): Duration = Duration.ofMillis((c * a.toMillis).toLong)
  }

  implicit class RichRandom(val rand: Random) extends AnyVal {

    /**
      * Samples a random value from a Gaussian distribution, works with any [[VectorSpace]], e.g.
      * you can sample random `Duration` instances.
      */
    def nextGaussian[T: VectorSpace](dist: GaussianDistribution[T]): T = {
      val vs = implicitly[VectorSpace[T]]
      vs.add(vs.multiply(rand.nextGaussian(), dist.stdDev), dist.mean)
    }
  }
}
