package com.socrata.datacoordinator.service

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.typesafe.config.Config

class CuratorConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val ensemble = config.getStringList(k("ensemble")).asScala.mkString(",")
  val sessionTimeout = config.getMilliseconds(k("session-timeout")).longValue.millis
  val connectTimeout = config.getMilliseconds(k("connect-timeout")).longValue.millis
  val maxRetries = config.getInt(k("max-retries"))
  val baseRetryWait = config.getMilliseconds(k("base-retry-wait")).longValue.millis
  val maxRetryWait = config.getMilliseconds(k("max-retry-wait")).longValue.millis
  val namespace = config.getString(k("namespace"))
}
