package com.socrata.datacoordinator.service

import java.io.File

import com.typesafe.config.Config

class SecondaryConfig(config: Config) {
  val configs = config.getObject("configs")
  val path = new File(config.getString("path"))
}
