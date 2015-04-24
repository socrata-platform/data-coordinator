package com.socrata.querycoordinator

import java.util.concurrent.TimeUnit

import com.socrata.http.server.livenesscheck.LivenessCheckConfig

import scala.concurrent.duration._

import com.socrata.thirdparty.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.thirdparty.metrics.MetricsOptions
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class QueryCoordinatorConfig(config: Config, root: String) extends ConfigClass(config, root) with SecondarySelectorConfig {

  val log4j = getRawConfig("log4j")
  val curator = new CuratorConfig(config, path("curator"))
  val discovery = new DiscoveryConfig(config, path("service-advertisement"))
  val livenessCheck = new LivenessCheckConfig(config, path("liveness-check"))
  val network = new NetworkConfig(config, path("network"))
  val metrics = MetricsOptions(config.getConfig(path("metrics")))

  val connectTimeout = config.getDuration(path("connect-timeout"), TimeUnit.MILLISECONDS).millis
  val schemaTimeout = config.getDuration(path("get-schema-timeout"), TimeUnit.MILLISECONDS).millis
  val queryTimeout = config.getDuration(path("query-timeout"), TimeUnit.MILLISECONDS).millis
  val maxRows = optionally(getInt("max-rows"))
  val defaultRowsLimit = getInt("default-rows-limit")

  val threadpool = getRawConfig("threadpool")
}
