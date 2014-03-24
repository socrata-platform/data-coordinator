package com.socrata.datacoordinator.service

import java.io.File
import scala.collection.JavaConverters._

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class SecondaryGroupConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val numReplicas = getInt("num-replicas")
  val instances = getStringList("instances").toSet
}

class SecondaryInstanceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val secondaryType = getString("type")
  val secondaryConfig = getRawConfig("config")
}

class SecondaryConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val path = new File(getString("path"))
  val defaultGroups = getStringList("default-groups").toSet
  val groups = config.getObject(path("groups")).keySet.asScala.toSeq.map { k =>
    k -> getConfig("groups." + k, new SecondaryGroupConfig(_, _))
  }.toMap
  val instances = config.getObject(path("instances")).keySet.asScala.toSeq.map { k =>
    k -> getConfig("instances." + k, new SecondaryInstanceConfig(_, _))
  }.toMap
}
