package com.socrata.querycoordinator

import com.typesafe.config.Config

class NetworkConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s

  val port = config.getInt(k("port"))
}
