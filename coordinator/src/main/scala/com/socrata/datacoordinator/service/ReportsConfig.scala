package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import java.io.File

import com.socrata.thirdparty.typesafeconfig.ConfigClass

class ReportsConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val directory = new File(getString("directory")).getAbsoluteFile
  val indexBlockSize = getBytes("index-block-size").intValue
  val dataBlockSize = getBytes("data-block-size").intValue
}
