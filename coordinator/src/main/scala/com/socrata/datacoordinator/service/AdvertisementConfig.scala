package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class AdvertisementConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val basePath = getString("base-path")
  val name = getString("name")
  val address = getString("address")
}
