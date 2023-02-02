package com.socrata.datacoordinator.secondary.config

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config
import java.io.File

trait StoreConfig {
  val storeCapacityMB: Long
  val acceptingNewDatasets: Boolean
}

class StoreConfigCfg(config: Config, root: String) extends ConfigClass(config, root) with StoreConfig {
  override val storeCapacityMB = getInt("storeCapacityMB").toLong
  override val acceptingNewDatasets = getBoolean("acceptingNewDatasets")
}

trait SecondaryGroupConfig {
  val numReplicas: Int
  val respectsCollocation: Boolean // Only pg respects collocation
  val instances: Map[String, StoreConfig]
}

class SecondaryGroupConfigCfg(config: Config, root: String) extends ConfigClass(config, root) with SecondaryGroupConfig {
  override val numReplicas = getInt("numReplicas")
  override val instances = getObjectOf[StoreConfig]("instances", new StoreConfigCfg(_, _))
  override val respectsCollocation = optionally(getBoolean("respectsCollocation")).getOrElse(true)
}

class SecondaryInstanceConfig(cfg: Config, root: String) extends ConfigClass(cfg, root) {
  val secondaryType = getString("secondaryType")
  val numWorkers = getInt("numWorkers")
  val config = getRawConfig("config")
}

class SecondaryConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val defaultGroups = getStringList("defaultGroups").toSet
  val groups = getObjectOf[SecondaryGroupConfig]("groups", new SecondaryGroupConfigCfg(_, _))
  val instances = getObjectOf("instances", new SecondaryInstanceConfig(_, _))
}
