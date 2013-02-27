package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.DataWritingContext

class DatasetCreator[T](dataWritingContext: DataWritingContext) {
  import dataWritingContext.datasetMutator._

  def createDataset(datasetId: String, username: String) {
    creatingDataset(as = username)(datasetId, "t") { ctx =>
      dataWritingContext.addSystemColumns(ctx)
    }
  }
}
