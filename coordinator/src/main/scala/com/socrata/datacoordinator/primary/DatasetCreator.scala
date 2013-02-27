package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.DataWritingContext

class DatasetCreator[T](dataWritingContext: DataWritingContext) {
  import dataWritingContext.datasetMutator

  def createDataset(datasetId: String, username: String) {
    for(ctx <- datasetMutator.createDataset(as = username)(datasetId, "t"))
      dataWritingContext.addSystemColumns(ctx)
  }
}
