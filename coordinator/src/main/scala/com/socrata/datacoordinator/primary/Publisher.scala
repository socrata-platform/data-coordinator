package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo

class Publisher(mutator: DatasetMutator[_, _]) extends ExistingDatasetMutator {
  def publish(dataset: String, snapshotsToKeep: Option[Int], username: String): UnanchoredCopyInfo = {
    finish(dataset) {
      mutator.publishCopy(as = username)(dataset, snapshotsToKeep).map {
        case mutator.CopyOperationComplete(ctx) =>
          Some(ctx.copyInfo.unanchored)
        case _ =>
          None
      }
    }
  }
}
