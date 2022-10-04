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

class MoverConfig(val config: Config, root: String, hostPort: Int => Int) extends ConfigClass(config, root) {
  val from = getConfig("from", new InstanceConfig(_, _))
  val to = getConfig("to", new InstanceConfig(_, _))
  val sodaFountain = getConfig("soda-fountain", new DataSourceConfig(_, _))
  val secondaries = locally {
    val secondaries = getRawConfig("secondaries")
    secondaries.root.entrySet.iterator.asScala.foldLeft(Map.empty[String, DataSourceConfig]) { (acc, ent) =>
      acc + (ent.getKey -> new DataSourceConfig(config, root + "." + ConfigUtil.joinPath("secondaries", ent.getKey)))
    }
  }

  val logProperties = getRawConfig("log4j")
  val tablespace = getString("tablespace")
  val writeLockTimeout = getDuration("write-lock-timeout")
  val acceptableSecondaries = getStringList("acceptable-secondaries").toSet
}

class InstanceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val instance = getString("instance")
  val dataSource = getConfig("database", new DataSourceConfig(_, _))

  require(instance.matches("[a-zA-Z0-9._]+"),
          "Instance names must consist of only ASCII letters, numbers, periods, and underscores")
}
