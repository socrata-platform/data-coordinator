package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.truth.metadata.CopyInfo

trait Truncator {
  def truncate(table: CopyInfo, logger: Logger[_, _]): Int
}
