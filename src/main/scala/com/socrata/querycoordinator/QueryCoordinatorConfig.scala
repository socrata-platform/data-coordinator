package com.socrata.querycoordinator

import scala.concurrent.duration._

import com.typesafe.config.{ConfigException, Config}
import scala.collection.JavaConversions.asScalaIterator


class QueryCoordinatorConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s

  val log4j = config.getConfig(k("log4j"))
  val curator = new CuratorConfig(config, k("curator"))
  val advertisement = new AdvertisementConfig(config, k("service-advertisement"))
  val network = new NetworkConfig(config, k("network"))

  val connectTimeout = config.getMilliseconds(k("connect-timeout")).longValue.millis
  val schemaTimeout = config.getMilliseconds(k("get-schema-timeout")).longValue.millis
  val queryTimeout = config.getMilliseconds(k("query-timeout")).longValue.millis
  val maxRows = try { Some(config.getInt(k("max-rows"))) } catch { case _: ConfigException.Missing => None}

  val allSecondaryInstanceNames = asScalaIterator(config.getStringList(k("all-secondary-instance-names")).iterator()).toSeq
  val secondaryDiscoveryExpirationMillis = config.getMilliseconds(k("secondary-discovery-expiration"))
  val datasetMaxNopeCount = config.getInt(k("dataset-max-nope-count"))
}