package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo

class Publisher(mutator: DatasetMutator[_]) extends ExistingDatasetMutator {
  def publish(dataset: String, username: String): UnanchoredCopyInfo = {
    finish(dataset) {
      for {
        ctxOpt <- mutator.publishCopy(as = username)(dataset)
        ctx <- ctxOpt
      } yield {
        ctx.copyInfo.unanchored
      }
    }
  }
}
