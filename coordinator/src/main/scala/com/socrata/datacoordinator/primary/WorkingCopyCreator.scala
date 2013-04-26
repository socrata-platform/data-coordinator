package com.socrata.datacoordinator.primary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}

class WorkingCopyCreator(universe: Managed[Universe[_, _] with DatasetMutatorProvider]) extends ExistingDatasetMutator {
  def copyDataset(dataset: String, username: String, copyData: Boolean): UnanchoredCopyInfo = {
    finish(dataset) {
      for {
        u <- universe
        ctx <- u.datasetMutator.createCopy(as = username)(dataset, copyData, _ => ())
      } yield ctx match {
        case u.datasetMutator.CopyOperationComplete(ctx) =>
          Some(ctx.copyInfo.unanchored)
        case _ =>
          None
      }
    }
  }
}
