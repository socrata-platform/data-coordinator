package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import java.io.File
import net.ceedubs.ficus.FicusConfig._

case class SecondaryGroupConfig(
     numReplicas: Int,
     instances: Set[String]
 )

case class SecondaryInstanceConfig(
    secondaryType: String,
    config: Config
)

class SecondaryConfig(config: Config) {
  val path = new File(config.as[String]("path"))
  val defaultGroups = config.as[Set[String]]("defaultGroups")
  val groups = config.as[Map[String, SecondaryGroupConfig]]("groups")
  val instances = config.as[Map[String, SecondaryInstanceConfig]]("instances")
}
