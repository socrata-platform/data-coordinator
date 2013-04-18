package com.socrata.datacoordinator.service

import com.typesafe.config.Config

class NetworkConfig(config: Config) {
  val port = config.getInt("port")
}
