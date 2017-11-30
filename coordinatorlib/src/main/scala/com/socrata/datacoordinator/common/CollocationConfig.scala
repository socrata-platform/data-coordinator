package com.socrata.datacoordinator.common

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class CollocationConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val group = getStringList("group").toSet
  val lockPath = getString("lock-path")
  val lockTimeout = getDuration("lock-timeout")
}
