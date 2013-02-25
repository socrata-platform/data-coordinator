package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.soql.brita.AsciiIdentifierFilter
import com.socrata.datacoordinator.truth.{DataWritingContext, MonadicDatasetMutator}

class DatasetCreator(dataWritingContext: DataWritingContext[_, _]) {
  import dataWritingContext.datasetMutator._

  def createDataset(datasetId: String, username: String): IO[Unit] = {
    creatingDataset(as = username)(datasetId, "t") {
      dataWritingContext.addSystemColumns
    }
  }
}
