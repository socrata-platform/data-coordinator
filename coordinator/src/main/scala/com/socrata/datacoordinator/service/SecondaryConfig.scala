package com.socrata.datacoordinator.service

import java.io.File

import com.typesafe.config.Config
import net.ceedubs.ficus.FicusConfig._

case class SecondaryInstanceConfig(
    secondaryType: String,
    numWorkers: Int,
    config: Config
)

class SecondaryConfig(config: Config) {
  val path = new File(config.as[String]("path"))
  val instances = config.as[Map[String, SecondaryInstanceConfig]]("instances")
}
