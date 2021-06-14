/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.hadoop

import com.adform.streamloader.util.JsonSerializer
import org.json4s.JArray
import org.json4s.JsonAST.{JObject, JString, JValue}

case class FileStaging(stagingPath: String, destinationPath: String)

case class MultiFileStaging(fileStagings: Seq[FileStaging])

object MultiFileStaging {

  implicit val jsonSerializer: JsonSerializer[MultiFileStaging] = new JsonSerializer[MultiFileStaging] {

    override def serialize(value: MultiFileStaging): JValue = JArray(
      value.fileStagings
        .map(fs => {
          JObject("staged_file_path" -> JString(fs.stagingPath), "destination_file_path" -> JString(fs.destinationPath))
        })
        .toList
    )

    override def deserialize(json: JValue): MultiFileStaging = json match {
      case JArray(items) =>
        val stagings = items.map {
          case JObject(fields) =>
            val values = fields.toMap
            FileStaging(extractString(values("staged_file_path")), extractString(values("destination_file_path")))
          case o => throw new IllegalArgumentException(s"Object expected, but '$o' found")
        }
        MultiFileStaging(stagings)
      case o => throw new IllegalArgumentException(s"Array expected, but '$o' found")
    }
  }

  private def extractString(v: JValue): String = v match {
    case JString(s) => s
    case _ => throw new IllegalArgumentException(s"Expected a string, but found '$v'")
  }
}
