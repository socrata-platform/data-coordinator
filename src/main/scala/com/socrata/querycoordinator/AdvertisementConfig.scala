package com.socrata.querycoordinator

import com.typesafe.config.Config

class AdvertisementConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s

  val basePath = config.getString(k("base-path"))
  val name = config.getString(k("name"))
  val address = config.getString(k("address"))
}
