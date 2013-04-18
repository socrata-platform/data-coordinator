package com.socrata.querycoordinator

import com.typesafe.config.Config

class NetworkConfig(config: Config) {
  val port = config.getInt("port")
}
