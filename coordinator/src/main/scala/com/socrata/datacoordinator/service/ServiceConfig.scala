package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.secondary.config.SecondaryConfig
import com.socrata.datacoordinator.common.collocation.CollocationConfig
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.thirdparty.metrics.MetricsOptions

class ServiceConfig(val config: Config, root: String, hostPort: Int => Int) extends ConfigClass(config, root) {
  val collocation = getConfig("collocation", new CollocationConfig(_, _))
  val secondary = getConfig("secondary", new SecondaryConfig(_, _))
  val network = getConfig("network", new NetworkConfig(_, _))
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val discovery = getConfig("service-advertisement", new DiscoveryConfig(_, _))
  val livenessCheck = getConfig("liveness-check", new LivenessCheckConfig(_, _))
  val dataSource = getConfig("database", new DataSourceConfig(_, _))
  val logProperties = getRawConfig("log4j")
  val commandReadLimit = getBytes("command-read-limit")
  val allowDdlOnPublishedCopies = getBoolean("allow-ddl-on-published-copies")
  val instance = getString("instance")
  val tablespace = getString("tablespace")
  val writeLockTimeout = getDuration("write-lock-timeout")
  val reports = getConfig("reports", new ReportsConfig(_, _))
  val metrics = MetricsOptions(getRawConfig("metrics")) // TODO make that a ConfigClass
  val logTableCleanupSleepTime = getDuration("log-table-cleanup-sleep-time")
  val logTableCleanupDeleteOlderThan = getDuration("log-table-cleanup-delete-older-than")
  val logTableCleanupDeleteEvery = getDuration("log-table-cleanup-delete-every")

  // if the thread count reaches this number, the mutation
  // registration will be removed.
  val maxMutationThreadsHighWater = getInt("max-mutation-threads-high-water")
  // ..and when it drops below this level, it will be re-enabled.
  val maxMutationThreadsLowWater = getInt("max-mutation-threads-low-water")

  val mutationResourceTimeout = getDuration("mutation-resource-timeout")
  val jettyThreadpool = getRawConfig("jetty-threadpool") // TODO make that a ConfigClass

  require(instance.matches("[a-zA-Z0-9._]+"),
          "Instance names must consist of only ASCII letters, numbers, periods, and underscores")
}
