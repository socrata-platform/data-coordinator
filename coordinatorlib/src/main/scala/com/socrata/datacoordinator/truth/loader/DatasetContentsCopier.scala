package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, CopyInfo}

trait DatasetContentsCopier[CT] {
  def copy(from: CopyInfo, to: DatasetCopyContext[CT])
}
