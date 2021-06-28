/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.batch.storage

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.{JsonSerializer, Logging}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.control.NonFatal

/**
  * Two-phase commit staging information, part of the kafka offset commit metadata.
  * The staging information contains the staged record range start and end positions and
  * some batch dependent storage information (e.g. staged temp file path), which must be JSON serializable.
  */
case class StagedOffsetCommit[S: JsonSerializer](staging: S, start: StreamPosition, end: StreamPosition)

/**
  * Kafka commit metadata used in the two-phase commit implementation, stored for each topic partition separately.
  * Consists of the currently stored watermark and an optional staging part, if a commit is in progress.
  */
case class TwoPhaseCommitMetadata[S: JsonSerializer](
    watermark: Timestamp,
    stagedOffsetCommit: Option[StagedOffsetCommit[S]]) {

  def toJson: String = {
    val stagingJson = stagedOffsetCommit.map(
      s =>
        ("staging" -> implicitly[JsonSerializer[S]].serialize(s.staging))
          ~ ("start_offset" -> s.start.offset)
          ~ ("start_watermark" -> s.start.watermark.millis)
          ~ ("end_offset" -> s.end.offset)
          ~ ("end_watermark" -> s.end.watermark.millis)
    )
    val watermarkJson: JObject = "watermark" -> watermark.millis
    val json = if (stagingJson.isDefined) watermarkJson ~ ("staged" -> stagingJson.get) else watermarkJson
    compact(render(json))
  }
}

object TwoPhaseCommitMetadata extends Logging {

  implicit val formats: Formats = DefaultFormats

  def tryParseJson[S: JsonSerializer](metadata: String): Option[TwoPhaseCommitMetadata[S]] =
    try {
      val json = parse(metadata)
      val fields = extractObjProps(json)
      val watermark = Timestamp(extractLong(fields("watermark")))
      val staged = fields
        .get("staged")
        .map(extractObjProps)
        .map(props => {
          StagedOffsetCommit(
            implicitly[JsonSerializer[S]].deserialize(props("staging")),
            StreamPosition(
              extractLong(props("start_offset")),
              Timestamp(extractLong(props("start_watermark")))
            ),
            StreamPosition(
              extractLong(props("end_offset")),
              Timestamp(extractLong(props("end_watermark")))
            )
          )
        })
      Some(TwoPhaseCommitMetadata(watermark, staged))
    } catch {
      case NonFatal(e) =>
        log.error(e)("Could not parse commit metadata")
        None
    }

  private def extractObjProps(value: JValue): Map[String, JValue] = value match {
    case JObject(o) => o.obj.toMap
    case _ => throw new IllegalArgumentException(s"Object expected, but found '$value'")
  }

  private def extractLong(value: JValue): Long = value match {
    case JInt(num) => num.toLong
    case JLong(num) => num
    case _ => throw new IllegalArgumentException(s"Long expected, but found '$value'")
  }
}
