package com.socrata.datacoordinator.common.collocation

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class CollocationConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val group = getStringList("group").toSet
  val lockPath = getString("lock-path")
  val lockTimeout = getDuration("lock-timeout")
}
