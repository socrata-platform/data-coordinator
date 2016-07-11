package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.secondary.config.SecondaryConfig
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{MILLISECONDS, FiniteDuration, Duration}

class ServiceConfig(val config: Config, root: String) {
  private def k(field: String) = root + "." + field
  val secondary = new SecondaryConfig(config.getConfig(k("secondary")))
  val network = new NetworkConfig(config, k("network"))
  val curator = new CuratorConfig(config, k("curator"))
  val discovery = new DiscoveryConfig(config, k("service-advertisement"))
  val livenessCheck = new LivenessCheckConfig(config, k("liveness-check"))
  val dataSource = new DataSourceConfig(config, k("database"))
  val logProperties = config.getConfig(k("log4j"))
  val commandReadLimit = config.getBytes(k("command-read-limit")).longValue
  val allowDdlOnPublishedCopies = config.getBoolean(k("allow-ddl-on-published-copies"))
  val instance = config.getString(k("instance"))
  val tablespace = config.getString(k("tablespace"))
  val writeLockTimeout = new FiniteDuration(config.getDuration(k("write-lock-timeout"), MILLISECONDS),
                                            TimeUnit.MILLISECONDS)
  val reports = new ReportsConfig(config, k("reports"))
  val metrics = config.getConfig(k("metrics"))
  val logTableCleanupSleepTime = new FiniteDuration(config.getDuration(k("log-table-cleanup-sleep-time"), MILLISECONDS),
                                                    TimeUnit.MILLISECONDS)
  val logTableCleanupDeleteOlderThan = new FiniteDuration(
                                         config.getDuration(k("log-table-cleanup-delete-older-than"), MILLISECONDS),
                                         TimeUnit.MILLISECONDS)
  val logTableCleanupDeleteEvery = new FiniteDuration(
                                     config.getDuration(k("log-table-cleanup-delete-every"), MILLISECONDS),
                                     TimeUnit.MILLISECONDS)
  //val tableCleanupDelay = new FiniteDuration (config.getDuration (k("table-cleanup-delay"), MILLISECONDS), TimeUnit.MILLISECONDS)
  val maxMutationThreads = config.getInt(k("max-mutation-threads"))
  val mutationResourceTimeout = new FiniteDuration(config.getDuration(k("mutation-resource-timeout"),
                                                   MILLISECONDS), TimeUnit.MILLISECONDS)
  val jettyThreadpool = config.getConfig(k("jetty-threadpool"))
  require(instance.matches("[a-zA-Z0-9._]+"),
          "Instance names must consist of only ASCII letters, numbers, periods, and underscores")
}
