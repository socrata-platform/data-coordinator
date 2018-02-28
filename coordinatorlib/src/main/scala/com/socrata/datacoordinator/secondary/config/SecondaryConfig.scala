package com.socrata.datacoordinator.secondary.config

import com.typesafe.config.Config
import java.io.File
import net.ceedubs.ficus.FicusConfig._

// TODO: rewrite this to use our config classes instead of this
// reflection-based horror that doesn't even provide full paths to
// errors.

case class StoreConfig(storeCapacityMB: Long, acceptingNewDatasets: Boolean)

case class SecondaryGroupConfig(
     numReplicas: Int,
     instances: Map[String, StoreConfig]
 )

case class SecondaryInstanceConfig(
    secondaryType: String,
    numWorkers: Int,
    config: Config
)

class SecondaryConfig(config: Config) {
  val defaultGroups = config.as[Set[String]]("defaultGroups")
  val groups = config.as[Map[String, SecondaryGroupConfig]]("groups")
  val instances = config.as[Map[String, SecondaryInstanceConfig]]("instances")
}
