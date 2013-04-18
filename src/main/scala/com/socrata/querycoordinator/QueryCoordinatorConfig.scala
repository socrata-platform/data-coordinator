package com.socrata.querycoordinator

import com.typesafe.config.Config

class QueryCoordinatorConfig(config: Config) {
  val log4j = config.getConfig("log4j")
  val curator = new CuratorConfig(config.getConfig("curator"))
  val advertisement = new AdvertisementConfig(config.getConfig("service-advertisement"))
  val network = new NetworkConfig(config.getConfig("network"))
}
