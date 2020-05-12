/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.file.storage

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.util.Logging
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.control.NonFatal

/**
  * Information about a file staging, i.e. where is it staged and where should it be stored.
  * The path strings can be arbitrary and not necessarily actual file paths,
  * e.g. when implementing S3 storage the staging path could be a file upload ID.
  */
case class FileStaging(stagingPath: String, destinationPath: String)

/**
  * Information about a staged file commit.
  */
case class StagedOffsetCommit(fileStaging: FileStaging, start: StreamPosition, end: StreamPosition)

/**
  * Metadata written to Kafka when committing offsets.
  *
  * @param watermark Watermark of the current offset commit.
  * @param stagedOffsetCommit Staging information, if any.
  */
case class TwoPhaseCommitMetadata(watermark: Timestamp, stagedOffsetCommit: Option[StagedOffsetCommit]) {
  def toJson: String = {
    val stagingJson = stagedOffsetCommit.map(
      s =>
        ("staged_file_path" -> s.fileStaging.stagingPath)
          ~ ("destination_file_path" -> s.fileStaging.destinationPath)
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
  def tryParseJson(metadata: String): Option[TwoPhaseCommitMetadata] =
    try {
      val json = parse(metadata)
      val fields = json.asInstanceOf[JObject].values
      val watermark = Timestamp(fields("watermark").asInstanceOf[BigInt].toLong)
      val staged = fields.get("staged").map(_.asInstanceOf[Map[String, Any]]).map { props =>
        StagedOffsetCommit(
          FileStaging(props("staged_file_path").toString, props("destination_file_path").toString),
          StreamPosition(
            props("start_offset").asInstanceOf[BigInt].toLong,
            Timestamp(props("start_watermark").asInstanceOf[BigInt].toLong)
          ),
          StreamPosition(
            props("end_offset").asInstanceOf[BigInt].toLong,
            Timestamp(props("end_watermark").asInstanceOf[BigInt].toLong)
          )
        )
      }
      Some(TwoPhaseCommitMetadata(watermark, staged))
    } catch {
      case NonFatal(e) =>
        log.error(e)("Could not parse commit metadata")
        None
    }
}
