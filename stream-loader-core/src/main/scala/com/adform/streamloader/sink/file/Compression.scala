/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

/**
  * Common compression type enumeration.
  */
sealed trait Compression {

  /**
    * Gets the file extension that is used for this compression type, if any.
    */
  def fileExtension: Option[String]
}

object Compression {

  case object NONE extends Compression {
    override def fileExtension: Option[String] = None
  }

  case object ZSTD extends Compression {
    override def fileExtension: Option[String] = Some("zst")
  }

  case object GZIP extends Compression {
    override def fileExtension: Option[String] = Some("gz")
  }

  case object BZIP extends Compression {
    override def fileExtension: Option[String] = Some("bz2")
  }

  case object LZOP extends Compression {
    override def fileExtension: Option[String] = Some("lzo")
  }

  case object SNAPPY extends Compression {
    override def fileExtension: Option[String] = Some("snappy")
  }

  case object LZ4 extends Compression {
    override def fileExtension: Option[String] = Some("lz4")
  }

  def tryParse(str: String): Option[Compression] = {
    str.toLowerCase match {
      case "none" => Some(Compression.NONE)
      case "zstd" => Some(Compression.ZSTD)
      case "gzip" => Some(Compression.GZIP)
      case "bzip" => Some(Compression.BZIP)
      case "lzop" => Some(Compression.LZOP)
      case "snappy" => Some(Compression.SNAPPY)
      case "lz4" => Some(Compression.LZ4)
      case _ => None
    }
  }
}
