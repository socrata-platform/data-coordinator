package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.DataWritingContext

class DatasetCreator[T](dataWritingContext: DataWritingContext) {
  import dataWritingContext.datasetMutator

  def createDataset(datasetId: String, username: String, localeName: String) {
    for {
      ctxOpt <- datasetMutator.createDataset(as = username)(datasetId, "t", localeName)
      ctx <- ctxOpt
    } dataWritingContext.addSystemColumns(ctx)
  }
}
