package com.socrata.querycoordinator

import com.socrata.http.server.livenesscheck.LivenessCheckConfig

import scala.concurrent.duration._

import com.socrata.thirdparty.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.thirdparty.metrics.MetricsOptions
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class QueryCoordinatorConfig(config: Config, root: String) extends ConfigClass(config, root) {

  val log4j = getRawConfig("log4j")
  val curator = new CuratorConfig(config, path("curator"))
  val discovery = new DiscoveryConfig(config, path("service-advertisement"))
  val livenessCheck = new LivenessCheckConfig(config, path("liveness-check"))
  val network = new NetworkConfig(config, path("network"))
  val metrics = MetricsOptions(config.getConfig(path("metrics")))

  val connectTimeout = config.getMilliseconds(path("connect-timeout")).longValue.millis
  val schemaTimeout = config.getMilliseconds(path("get-schema-timeout")).longValue.millis
  val queryTimeout = config.getMilliseconds(path("query-timeout")).longValue.millis
  val maxRows = optionally(getInt("max-rows"))
  val defaultRowsLimit = getInt("default-rows-limit")

  val allSecondaryInstanceNames = getStringList("all-secondary-instance-names")
  val secondaryDiscoveryExpirationMillis = config.getMilliseconds(path("secondary-discovery-expiration"))
  val datasetMaxNopeCount = getInt("dataset-max-nope-count")
}
