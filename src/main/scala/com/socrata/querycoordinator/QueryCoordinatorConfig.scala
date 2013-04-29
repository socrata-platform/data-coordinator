package com.socrata.querycoordinator

import scala.concurrent.duration._

import com.typesafe.config.Config

class QueryCoordinatorConfig(config: Config) {
  val log4j = config.getConfig("log4j")
  val curator = new CuratorConfig(config.getConfig("curator"))
  val advertisement = new AdvertisementConfig(config.getConfig("service-advertisement"))
  val network = new NetworkConfig(config.getConfig("network"))

  val schemaTimeout = config.getMilliseconds("get-schema-timeout").longValue.millis
  val initialResponseTimeout = config.getMilliseconds("initial-response-timeout").longValue.millis
  val responseDataTimeout = config.getMilliseconds("response-data-timeout").longValue.millis
}
