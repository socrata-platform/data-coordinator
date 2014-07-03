package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CuratorConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val ensemble = config.getStringList(k("ensemble")).asScala.mkString(",")
  val sessionTimeout = config.getDuration(k("session-timeout"), MILLISECONDS).longValue.millis
  val connectTimeout = config.getDuration(k("connect-timeout"), MILLISECONDS).longValue.millis
  val maxRetries = config.getInt(k("max-retries"))
  val baseRetryWait = config.getDuration(k("base-retry-wait"), MILLISECONDS).longValue.millis
  val maxRetryWait = config.getDuration(k("max-retry-wait"), MILLISECONDS).longValue.millis
  val namespace = config.getString(k("namespace"))
}
