/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.model

import java.io.ByteArrayOutputStream
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, ScalePrecision}
import org.scalacheck.{Arbitrary, Gen}

import scala.math.BigDecimal.RoundingMode
import scala.util.Using

case class ExampleMessage(
    id: Int,
    name: String,
    timestamp: LocalDateTime,
    height: Double,
    width: Float,
    isEnabled: Boolean,
    childIds: Array[Int],
    parentId: Option[Long],
    transactionId: UUID,
    moneySpent: BigDecimal
) extends StorageMessage {

  private implicit val ROUNDING_MODE: RoundingMode.RoundingMode = ExampleMessage.ROUNDING_MODE
  private implicit val SCALE_PRECISION: ScalePrecision = ExampleMessage.SCALE_PRECISION

  def getBytes: Array[Byte] = {
    val output = new ByteArrayOutputStream()
    Using.resource(AvroOutputStream.binary[ExampleMessage].to(output).build()) { stream =>
      {
        stream.write(this)
        stream.flush()
        output.toByteArray
      }
    }
  }

  // The default equality implementation would compare arrays by reference,
  // which would break scalatest's "contain theSameElementsAs".
  // Also perform relative float/double comparisons, as e.g. Vertica can round off
  // the fractional part for some reason
  override def equals(obj: Any): Boolean = obj match {
    case that: ExampleMessage =>
      this.id == that.id &&
        this.name == that.name &&
        this.timestamp == that.timestamp &&
        math.abs(this.height - that.height) / this.height < 1E-10 &&
        math.abs(this.width - that.width) / this.width < 1E-10 &&
        this.isEnabled == that.isEnabled &&
        this.childIds.sameElements(that.childIds) &&
        this.parentId == that.parentId &&
        this.transactionId == that.transactionId &&
        this.moneySpent == that.moneySpent
    case _ => false
  }
}

object ExampleMessage {

  implicit val ROUNDING_MODE: RoundingMode.RoundingMode = RoundingMode.HALF_UP
  implicit val SCALE_PRECISION: ScalePrecision = ScalePrecision(6, 18)

  private val schema = AvroSchema[ExampleMessage]

  def parseFrom(bytes: Array[Byte]): ExampleMessage = {
    Using.resource(AvroInputStream.binary[ExampleMessage].from(bytes).build(schema)) { stream =>
      stream.iterator.next()
    }
  }

  private implicit val arbString: Arbitrary[String] = Arbitrary(Gen.alphaNumStr.suchThat(_.length < 500))

  private implicit val arbTime: Arbitrary[LocalDateTime] = Arbitrary(
    Gen
      .choose(946684800000L, 1893456000000L) // 2020-01-01 to 2030-01-01
      .map(v => LocalDateTime.ofInstant(Instant.ofEpochMilli(v), ZoneId.of("UTC")))
  )

  private implicit val arbDecimal: Arbitrary[BigDecimal] = Arbitrary(
    Gen.choose(-10000000000L, 10000000000L).map(u => BigDecimal(u, SCALE_PRECISION.scale))
  )

  implicit val arbMessage: Arbitrary[ExampleMessage] = Arbitrary(Gen.resultOf(ExampleMessage.apply _))
}
