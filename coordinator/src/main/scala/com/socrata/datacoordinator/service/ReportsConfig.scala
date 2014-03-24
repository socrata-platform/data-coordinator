package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import java.io.File
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class ReportsConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val directory = new File(getString("directory")).getAbsoluteFile
  val indexBlockSize = config.getBytes(path("index-block-size")).intValue
  val dataBlockSize = config.getBytes(path("data-block-size")).intValue
}
