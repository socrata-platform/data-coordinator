package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo

class Publisher(mutator: DatasetMutator[_, _]) extends ExistingDatasetMutator {
  def publish(dataset: String, username: String): UnanchoredCopyInfo = {
    finish(dataset) {
      mutator.publishCopy(as = username)(dataset).map {
        case mutator.CopyOperationComplete(ctx) =>
          Some(ctx.copyInfo.unanchored)
        case _ =>
          None
      }
    }
  }
}
