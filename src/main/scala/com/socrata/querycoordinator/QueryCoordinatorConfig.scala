package com.socrata.querycoordinator

import scala.concurrent.duration._

import com.socrata.thirdparty.curator.CuratorConfig
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class QueryCoordinatorConfig(config: Config, root: String) extends ConfigClass(config, root) {

  val log4j = getRawConfig("log4j")
  val curator = new CuratorConfig(config, path("curator"))
  val advertisement = new AdvertisementConfig(config, path("service-advertisement"))
  val network = new NetworkConfig(config, path("network"))

  val connectTimeout = config.getMilliseconds(path("connect-timeout")).longValue.millis
  val schemaTimeout = config.getMilliseconds(path("get-schema-timeout")).longValue.millis
  val queryTimeout = config.getMilliseconds(path("query-timeout")).longValue.millis
  val maxRows = optionally(getInt("max-rows"))
  val defaultRowsLimit = optionally(getInt("default-rows-limit")).getOrElse(1000)

  val allSecondaryInstanceNames = getStringList("all-secondary-instance-names")
  val secondaryDiscoveryExpirationMillis = config.getMilliseconds(path("secondary-discovery-expiration"))
  val datasetMaxNopeCount = getInt("dataset-max-nope-count")
}
