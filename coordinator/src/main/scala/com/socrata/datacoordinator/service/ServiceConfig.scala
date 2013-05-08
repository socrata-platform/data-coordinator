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
  val commandReadLimit = config.getBytes("command-read-limit").longValue
  val allowDdlOnPublishedCopies = config.getBoolean("allow-ddl-on-published-copies")
  val instance = config.getString("instance")

  require(instance.matches("[a-zA-Z0-9._]+"), "Instance names must consist of only ASCII letters, numbers, periods, and underscores")
}
