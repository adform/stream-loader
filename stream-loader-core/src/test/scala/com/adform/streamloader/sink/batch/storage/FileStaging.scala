/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch.storage

import com.adform.streamloader.util.JsonSerializer
import org.json4s.JsonAST._

case class FileStaging(stagingPath: String, destinationPath: String)

object FileStaging {

  implicit val jsonSerializer: JsonSerializer[FileStaging] = new JsonSerializer[FileStaging] {
    override def serialize(value: FileStaging): JValue = JObject(
      "staged_file_path" -> JString(value.stagingPath),
      "destination_file_path" -> JString(value.destinationPath)
    )

    override def deserialize(json: JValue): FileStaging = json match {
      case JObject(fields) =>
        val values = fields.toMap
        FileStaging(
          extractString(values("staged_file_path")),
          extractString(values("destination_file_path"))
        )
      case _ =>
        throw new IllegalArgumentException("Invalid staging JSON")
    }

    private def extractString(v: JValue): String = v match {
      case JString(s) => s
      case _ => throw new IllegalArgumentException(s"Expected a string, but found '$v'")
    }
  }
}
