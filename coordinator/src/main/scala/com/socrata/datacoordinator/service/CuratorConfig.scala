package com.socrata.datacoordinator.service

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class CuratorConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val ensemble = getStringList("ensemble").mkString(",")
  val sessionTimeout = getDuration("session-timeout")
  val connectTimeout = getDuration("connect-timeout")
  val maxRetries = getInt("max-retries")
  val baseRetryWait = getDuration("base-retry-wait")
  val maxRetryWait = getDuration("max-retry-wait")
  val namespace = getString("namespace")
}
