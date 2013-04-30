package com.socrata.datacoordinator.primary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.id.DatasetId

class WorkingCopyCreator(universe: Managed[Universe[_, _] with DatasetMutatorProvider]) extends ExistingDatasetMutator {
  def copyDataset(dataset: DatasetId, username: String, copyData: Boolean): UnanchoredCopyInfo = {
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
