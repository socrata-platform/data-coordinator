package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.truth.metadata.CopyInfo

trait Truncator {
  def truncate[CV](table: CopyInfo, logger: Logger[CV])
}
