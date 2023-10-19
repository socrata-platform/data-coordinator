package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.secondary.config.{SecondaryConfig => ServiceSecondaryConfig}
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerConfig
import com.typesafe.config.Config
import java.util.UUID

import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.datacoordinator.common.collocation.CollocationConfig

import com.socrata.thirdparty.typesafeconfig.{ConfigClass, Propertizer}
import com.socrata.thirdparty.metrics.MetricsOptions

import scala.concurrent.duration._

trait SecondaryWatcherAppConfig {
  val backoffInterval: FiniteDuration
  val claimTimeout: FiniteDuration
  val collocationLockPath: String
  val collocationLockTimeout: FiniteDuration
  val instance: String
  val log4j: java.util.Properties
  val maxReplayWait: FiniteDuration
  val maxReplays: Option[Int]
  val maxRetries: Int
  val messageProducerConfig: Option[MessageProducerConfig]
  val replayWait: FiniteDuration
  val tmpdir: java.io.File
  val watcherId: UUID
}

class SecondaryWatcherConfig(config: Config, root: String) extends ConfigClass(config, root) with SecondaryWatcherAppConfig {
  val collocation = getConfig("collocation", new CollocationConfig(_, _))
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val database = getConfig("database", new DataSourceConfig(_, _))
  val discovery = getConfig("service-advertisement", new DiscoveryConfig(_, _))
  val metrics = MetricsOptions(getRawConfig("metrics")) // TODO: make this a ConfigClass
  val secondaryConfig = getConfig("secondary", new ServiceSecondaryConfig(_, _))

  override val backoffInterval = getDuration("backoff-interval")
  override val claimTimeout = getDuration("claim-timeout")
  override val collocationLockPath = collocation.lockPath
  override val collocationLockTimeout = collocation.lockTimeout
  override val instance = getString("instance")
  override val log4j = Propertizer("log4j", getRawConfig("log4j"))
  override val maxReplayWait = getDuration("max-replay-wait")
  override val maxReplays = optionally(getInt("max-replays"))
  override val maxRetries = getInt("max-retries")
  override val messageProducerConfig = optionally(getRawConfig("message-producer")).map { _ => getConfig("message-producer", new MessageProducerConfig(_, _))}
  override val replayWait = getDuration("replay-wait")
  override val tmpdir = new java.io.File(getString("tmpdir")).getAbsoluteFile
  override val watcherId = UUID.fromString(getString("watcher-id"))

}
