package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.datacoordinator.truth.DatasetMutator

class WorkingCopyCreator(mutator: DatasetMutator[_,_]) extends ExistingDatasetMutator {
  import mutator._
  def copyDataset(dataset: String, username: String, copyData: Boolean): UnanchoredCopyInfo = {
    finish(dataset) {
      createCopy(as = username)(dataset, copyData) map {
        case CopyOperationComplete(ctx) =>
          Some(ctx.copyInfo.unanchored)
        case _ =>
          None
      }
    }
  }
}
