package com.socrata.datacoordinator.secondary.config

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config
import java.io.File

trait StoreConfig {
  val storeCapacityMB: Long
  val acceptingNewDatasets: Boolean
}

class StoreConfigCfg(config: Config, root: String) extends ConfigClass(config, root) with StoreConfig {
  val storeCapacityMB = getInt("storeCapacityMB").toLong
  val acceptingNewDatasets = getBoolean("acceptingNewDatasets")
}

trait SecondaryGroupConfig {
  val numReplicas: Int
  val instances: Map[String, StoreConfig]
}

class SecondaryGroupConfigCfg(config: Config, root: String) extends ConfigClass(config, root) with SecondaryGroupConfig {
  val numReplicas = getInt("numReplicas")
  val instances = getObjectOf[StoreConfig]("instances", new StoreConfigCfg(_, _))
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
