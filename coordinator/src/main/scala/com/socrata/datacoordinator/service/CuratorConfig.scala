package com.socrata.datacoordinator.service

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.typesafe.config.Config

class CuratorConfig(config: Config) {
  val ensemble = config.getStringList("ensemble").asScala.mkString(",")
  val sessionTimeout = config.getMilliseconds("session-timeout").longValue.millis
  val connectTimeout = config.getMilliseconds("connect-timeout").longValue.millis
  val maxRetries = config.getInt("max-retries")
  val baseRetryWait = config.getMilliseconds("base-retry-wait").longValue.millis
  val maxRetryWait = config.getMilliseconds("max-retry-wait").longValue.millis
  val namespace = config.getString("namespace")
}
