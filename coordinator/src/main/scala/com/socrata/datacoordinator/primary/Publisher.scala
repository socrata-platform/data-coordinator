package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.MonadicDatasetMutator
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo

class Publisher(mutator: MonadicDatasetMutator[_]) extends ExistingDatasetMutator {
  def publish(dataset: String, username: String): UnanchoredCopyInfo = {
    finish(dataset) {
      mutator.withDataset(as = username)(dataset) { ctx =>
        ctx.publish().unanchored
      }
    }
  }
}
