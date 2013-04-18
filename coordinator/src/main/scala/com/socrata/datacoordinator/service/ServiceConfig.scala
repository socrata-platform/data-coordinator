package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import com.socrata.datacoordinator.common.DataSourceConfig

class ServiceConfig(val config: Config) {
  val secondary = new SecondaryConfig(config.getConfig("secondary"))
  val network = new NetworkConfig(config.getConfig("network"))
  val curator = new CuratorConfig(config.getConfig("curator"))
  val advertisement = new AdvertisementConfig(config.getConfig("service-advertisement"))
  val dataSource = new DataSourceConfig(config.getConfig("database"))
  val logProperties = config.getConfig("log4j")
}
