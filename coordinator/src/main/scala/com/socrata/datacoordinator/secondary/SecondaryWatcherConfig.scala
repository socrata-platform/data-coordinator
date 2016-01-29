package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.service.{SecondaryConfig => ServiceSecondaryConfig}
import com.typesafe.config.Config
import java.util.UUID
import scala.concurrent.duration._

class SecondaryWatcherConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val log4j = config.getConfig(k("log4j"))
  val database = new DataSourceConfig(config, k("database"))
  val secondaryConfig = new ServiceSecondaryConfig(config.getConfig(k("secondary")))
  val instance = config.getString(k("instance"))
  val metrics = config.getConfig(k("metrics"))
  val watcherId = UUID.fromString(config.getString(k("watcher-id")))
  val claimTimeout = config.getDuration(k("claim-timeout"), MILLISECONDS).longValue.millis
  val maxRetries = config.getInt(k("max-retries"))
  val maxReplays = try { Some(config.getInt(k("max-replays"))) } catch { case e: Throwable => None }
  val backoffInterval = config.getDuration(k("backoff-interval"), MILLISECONDS).longValue.millis
  val maxReplayWait = config.getDuration(k("max-replay-wait"), MILLISECONDS).longValue.millis
  val replayWait = config.getDuration(k("replay-wait"), MILLISECONDS).longValue.millis
  val tmpdir = new java.io.File(config.getString(k("tmpdir"))).getAbsoluteFile
}
