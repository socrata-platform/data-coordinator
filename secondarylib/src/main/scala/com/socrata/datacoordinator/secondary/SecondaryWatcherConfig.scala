package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.secondary.config.{SecondaryConfig => ServiceSecondaryConfig}
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerConfig
import com.typesafe.config.Config
import java.util.UUID

import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.datacoordinator.common.collocation.CollocationConfig

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.thirdparty.metrics.MetricsOptions

class SecondaryWatcherConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val log4j = getRawConfig("log4j")
  val database = getConfig("database", new DataSourceConfig(_, _))
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val discovery = getConfig("service-advertisement", new DiscoveryConfig(_, _))
  val secondaryConfig = getConfig("secondary", new ServiceSecondaryConfig(_, _))
  val instance = getString("instance")
  val collocation = getConfig("collocation", new CollocationConfig(_, _))
  val metrics = MetricsOptions(getRawConfig("metrics")) // TODO: make this a ConfigClass
  val watcherId = UUID.fromString(getString("watcher-id"))
  val claimTimeout = getDuration("claim-timeout")
  val maxRetries = getInt("max-retries")
  val maxReplays = optionally(getInt("max-replays"))
  val backoffInterval = getDuration("backoff-interval")
  val maxReplayWait = getDuration("max-replay-wait")
  val replayWait = getDuration("replay-wait")
  val tmpdir = new java.io.File(getString("tmpdir")).getAbsoluteFile
  val messageProducerConfig = optionally(getRawConfig("message-producer")).map { _ =>
    getConfig("message-producer", new MessageProducerConfig(_, _))
  }
}
