package com.socrata.datacoordinator.mover

import com.socrata.datacoordinator.secondary.config.SecondaryConfig
import com.socrata.datacoordinator.common.collocation.CollocationConfig
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit

import com.socrata.datacoordinator.service.ReportsConfig

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.thirdparty.metrics.MetricsOptions

class MoverConfig(val config: Config, root: String, hostPort: Int => Int) extends ConfigClass(config, root) {
  val from = getConfig("from", new InstanceConfig(_, _))
  val to = getConfig("to", new InstanceConfig(_, _))

  val logProperties = getRawConfig("log4j")
  val tablespace = getString("tablespace")
  val writeLockTimeout = getDuration("write-lock-timeout")
}

class InstanceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val instance = getString("instance")
  val dataSource = getConfig("database", new DataSourceConfig(_, _))

  require(instance.matches("[a-zA-Z0-9._]+"),
          "Instance names must consist of only ASCII letters, numbers, periods, and underscores")
}
