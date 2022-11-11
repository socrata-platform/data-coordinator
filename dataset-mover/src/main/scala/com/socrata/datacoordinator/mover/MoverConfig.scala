package com.socrata.datacoordinator.mover

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.secondary.config.SecondaryConfig
import com.socrata.datacoordinator.common.collocation.CollocationConfig
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.typesafe.config.{Config, ConfigUtil}
import java.util.concurrent.TimeUnit

import com.socrata.datacoordinator.service.ReportsConfig

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.thirdparty.metrics.MetricsOptions

class MoverConfig(val config: Config, root: String) extends ConfigClass(config, root) {
  private def confMap[T](subpath: String, extractor: (Config, String) => T): Map[String, T] = {
    val raw = getRawConfig(subpath)
    raw.root.entrySet.iterator.asScala.foldLeft(Map.empty[String, T]) { (acc, ent) =>
      acc + (ent.getKey -> extractor(config, root + "." + ConfigUtil.joinPath(subpath, ent.getKey)))
    }
  }

  val truths = confMap("truths", new DataSourceConfig(_, _))
  val pgSecondaries = confMap("pg-secondaries", new DataSourceConfig(_, _))
  val sodaFountain = getConfig("soda-fountain", new DataSourceConfig(_, _))

  val archivalUrl = optionally(getString("archival-url"))

  val logProperties = getRawConfig("log4j")
  val tablespace = getString("tablespace")
  val writeLockTimeout = getDuration("write-lock-timeout")
  val additionalAcceptableSecondaries = getStringList("additional-acceptable-secondaries").toSet
  val postSodaFountainUpdatePause = getDuration("post-soda-fountain-update-pause")
}
