/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.s3

import com.adform.streamloader.util.JsonSerializer
import org.json4s.{JArray, JObject, JString, JValue}

case class S3FileStaging(uploadId: String, uploadPartTag: String, destinationKey: String)

case class S3MultiFileStaging(fileUploads: Seq[S3FileStaging])

object S3MultiFileStaging {

  implicit val jsonSerializer: JsonSerializer[S3MultiFileStaging] = new JsonSerializer[S3MultiFileStaging] {
    override def serialize(value: S3MultiFileStaging): JValue = {
      JArray(
        value.fileUploads
          .map(
            fs =>
              JObject(
                "id" -> JString(fs.uploadId),
                "part_tag" -> JString(fs.uploadPartTag),
                "key" -> JString(fs.destinationKey)
            ))
          .toList)
    }
    override def deserialize(json: JValue): S3MultiFileStaging = json match {
      case JArray(items) =>
        S3MultiFileStaging(
          items.map {
            case JObject(obj) =>
              val kvs = obj.toMap
              S3FileStaging(
                kvs("id").asInstanceOf[JString].s,
                kvs("part_tag").asInstanceOf[JString].s,
                kvs("key").asInstanceOf[JString].s
              )
            case o => throw new IllegalArgumentException(s"Object expected, but '$o' found")
          }
        )
      case o => throw new IllegalArgumentException(s"Array expected, but '$o' found")
    }
  }
}
