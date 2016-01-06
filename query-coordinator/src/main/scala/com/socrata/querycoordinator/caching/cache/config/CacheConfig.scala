package com.socrata.querycoordinator.caching.cache.config

import java.io.File

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class CacheConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val realConfig: CacheImplConfig =
    optionally(getString("type")) match {
      case Some("filesystem") => new FilesystemCacheConfig(config, root)
      case Some("postgresql") => new PostgresqlCacheConfig(config, root)
      case Some(other) => sys.error("Unknown cache configuration type " + other)
      case None => NoopCacheConfig
    }
  val rowsPerWindow = getInt("rows-per-window")
  val maxWindows = getInt("max-windows")
  val cleanInterval = getDuration("clean-interval")
}

sealed trait CacheImplConfig

sealed trait ConcreteCacheImplConfig  extends CacheImplConfig { this: ConfigClass =>
  val atimeUpdateInterval = getDuration("atime-update-interval")
  val survivorCutoff = getDuration("survivor-cutoff")
  val assumeDeadCreateCutoff = getDuration("assume-dead-create-cutoff")
}

object NoopCacheConfig extends CacheImplConfig

class FilesystemCacheConfig(config: Config, rootPath: String) extends ConfigClass(config, rootPath) with ConcreteCacheImplConfig {
  val root = new File(getString("root"))
}

class PostgresqlCacheConfig(config: Config, root: String) extends ConfigClass(config, root) with ConcreteCacheImplConfig {
  val deleteDelay = getDuration("delete-delay")
  val db = getConfig("database", new DatabaseConfig(_, _))
}

class DatabaseConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val host = getString("host")
  val port = getInt("port")
  val database = getString("database")
  val username = getString("username")
  val password = getString("password")
}
