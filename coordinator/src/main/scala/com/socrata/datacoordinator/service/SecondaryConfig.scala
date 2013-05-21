package com.socrata.datacoordinator.service

import java.io.File

import com.typesafe.config.Config

class SecondaryConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val configs = config.getObject(k("configs"))
  val path = new File(config.getString(k("path")))
}
