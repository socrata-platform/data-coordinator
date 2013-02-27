package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class WorkingCopyCreator(mutator: MonadicDatasetMutator[_]) extends ExistingDatasetMutator {
  import mutator._
  def copyDataset(dataset: String, username: String, copyData: Boolean): UnanchoredCopyInfo = {
    finish(dataset) {
      for {
        ctxOpt <- createCopy(as = username)(dataset, copyData)
        ctx <- ctxOpt
      } yield {
        ctx.copyInfo.unanchored
      }
    }
  }
}
