/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch.storage

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.{JsonSerializer, Logging}
import com.github.luben.zstd.Zstd
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import java.util.Base64
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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
    stagedOffsetCommit: Option[StagedOffsetCommit[S]]
) {

  def toJson: String = {
    val stagingJson = stagedOffsetCommit.map(s =>
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

  /**
    * Serializes the metadata by converting it to JSON, compressing and base64 encoding.
    */
  def serialize: String = new String(Base64.getEncoder.encode(Zstd.compress(toJson.getBytes("UTF-8"))), "UTF-8")
}

object TwoPhaseCommitMetadata extends Logging {

  implicit val formats: Formats = DefaultFormats

  /**
    * Deserializes a given metadata string read from Kafka.
    * Attempts base64 decoding, if it fails tries fall-backing to parsing JSON directly for backwards compatibility.
    * If decoding succeeds the bytes are decompressed before parsing JSON.
    */
  def deserialize[S: JsonSerializer](metadata: String): Option[TwoPhaseCommitMetadata[S]] = {
    val (bytes, isCompressed) = Try(Base64.getDecoder.decode(metadata)) match {
      case Success(d) => (d, true)
      case Failure(e) =>
        log.warn(e)(s"Base64 decoding of metadata string failed, using original string '$metadata'")
        (metadata.getBytes("UTF-8"), false)
    }
    if (isCompressed) {
      Try(
        new String(Zstd.decompress(bytes, Zstd.getFrameContentSize(bytes, 0, bytes.length, false).toInt), "UTF-8")
      ) match {
        case Success(decompressed) => TwoPhaseCommitMetadata.tryParseJson[S](decompressed)
        case Failure(ex) =>
          log.error(ex)(s"Failed decompressing base64 encoded metadata '$metadata'")
          None
      }
    } else {
      TwoPhaseCommitMetadata.tryParseJson[S](new String(bytes, "UTF-8"))
    }
  }

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
