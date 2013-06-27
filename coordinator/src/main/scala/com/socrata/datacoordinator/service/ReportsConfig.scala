package com.socrata.datacoordinator.service

import com.typesafe.config.Config
import java.io.File

class ReportsConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s

  val directory = new File(config.getString(k("directory"))).getAbsoluteFile
  val indexBlockSize = config.getBytes(k("index-block-size")).intValue
  val dataBlockSize = config.getBytes(k("data-block-size")).intValue
}
