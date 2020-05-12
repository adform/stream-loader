/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import java.time.Duration
import java.util.Properties

import com.typesafe.config.Config

import scala.jdk.CollectionConverters._

object ConfigExtensions {

  implicit class RichConfig(cfg: Config) {
    def toProperties: Properties = {
      val props = new Properties()
      cfg.entrySet().asScala.foreach(e => props.put(e.getKey, e.getValue.unwrapped()))
      props
    }

    def getConfigOpt(path: String): Option[Config] = if (cfg.hasPath(path)) Some(cfg.getConfig(path)) else None
    def getStringOpt(path: String): Option[String] = if (cfg.hasPath(path)) Some(cfg.getString(path)) else None
    def getIntOpt(path: String): Option[Int] = if (cfg.hasPath(path)) Some(cfg.getInt(path)) else None
    def getLongOpt(path: String): Option[Long] = if (cfg.hasPath(path)) Some(cfg.getLong(path)) else None
    def getBytesOpt(path: String): Option[Long] = if (cfg.hasPath(path)) Some(cfg.getBytes(path)) else None
    def getDurationOpt(path: String): Option[Duration] = if (cfg.hasPath(path)) Some(cfg.getDuration(path)) else None
  }

  implicit class RichProperties(props: Properties) {
    def updated(key: String, update: String => String): Properties = {
      val newProps = new Properties()
      newProps.putAll(props)
      newProps.put(key, update(newProps.getProperty(key)))
      newProps
    }
  }
}
