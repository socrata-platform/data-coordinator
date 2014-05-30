package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.common.DataSourceConfig
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class ServiceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val secondary = getConfig("secondary", new SecondaryConfig(_, _))
  val network = getConfig("network", new NetworkConfig(_, _))
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val advertisement = getConfig("service-advertisement", new AdvertisementConfig(_, _))
  val dataSource = getConfig("database", new DataSourceConfig(_, _))
  val logProperties = getRawConfig("log4j")
  val commandReadLimit = config.getBytes(path("command-read-limit")).longValue
  val allowDdlOnPublishedCopies = config.getBoolean(path("allow-ddl-on-published-copies"))
  val instance = getString("instance")
  val tablespace = getString("tablespace")
  val writeLockTimeout = getDuration("write-lock-timeout")
  val reports = getConfig("reports", new ReportsConfig(_, _))
  val logTableCleanupSleepTime = getDuration("log-table-cleanup-sleep-time")
  val logTableCleanupDeleteOlderThan = getDuration("log-table-cleanup-delete-older-than")
  val logTableCleanupDeleteEvery = getDuration("log-table-cleanup-delete-every")

  require(instance.matches("[a-zA-Z0-9._]+"),
          "Instance names must consist of only ASCII letters, numbers, periods, and underscores")
}
