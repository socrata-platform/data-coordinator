package com.socrata.datacoordinator.service

import com.typesafe.config.Config

import com.socrata.thirdparty.typesafeconfig.ConfigClass

class NetworkConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
}
