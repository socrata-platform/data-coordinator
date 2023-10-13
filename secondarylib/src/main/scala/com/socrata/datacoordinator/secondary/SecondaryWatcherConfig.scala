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

import scala.concurrent.duration._

trait SecondaryConfig {
  val watcherId: UUID
  val metrics: MetricsOptions
  val collocationLockPath: String
  val instance: String
  val tmpdir: java.io.File
}

trait SecondaryWatcherAppConfig {
  val messageProducerConfig: Option[MessageProducerConfig]
  val claimTimeout: FiniteDuration
  val backoffInterval: FiniteDuration
  val replayWait: FiniteDuration
  val maxReplayWait: FiniteDuration
  val maxRetries: Int
  val maxReplays: Option[Int]
  val collocationLockTimeout: FiniteDuration
}

class SecondaryWatcherConfig(config: Config, root: String) extends ConfigClass(config, root) with SecondaryWatcherAppConfig with SecondaryConfig {
  val log4j = getRawConfig("log4j")
  val database = getConfig("database", new DataSourceConfig(_, _))
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val discovery = getConfig("service-advertisement", new DiscoveryConfig(_, _))
  val secondaryConfig = getConfig("secondary", new ServiceSecondaryConfig(_, _))
  val collocation = getConfig("collocation", new CollocationConfig(_, _))


  override val watcherId = UUID.fromString(getString("watcher-id"))
  override val metrics = MetricsOptions(getRawConfig("metrics")) // TODO: make this a ConfigClass
  override val collocationLockPath = collocation.lockPath
  override val instance = getString("instance")
  override val tmpdir = new java.io.File(getString("tmpdir")).getAbsoluteFile

  override val messageProducerConfig = optionally(getRawConfig("message-producer")).map { _ =>
    getConfig("message-producer", new MessageProducerConfig(_, _))}
  override val claimTimeout = getDuration("claim-timeout")
  override val backoffInterval = getDuration("backoff-interval")
  override val replayWait = getDuration("replay-wait")
  override val maxReplayWait = getDuration("max-replay-wait")
  override val maxRetries = getInt("max-retries")
  override val maxReplays = optionally(getInt("max-replays"))
  override val collocationLockTimeout = collocation.lockTimeout
}
