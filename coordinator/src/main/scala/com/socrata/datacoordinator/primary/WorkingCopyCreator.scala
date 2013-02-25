package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class WorkingCopyCreator(mutator: MonadicDatasetMutator[_]) extends ExistingDatasetMutator {
  import mutator._
  def copyDataset(dataset: String, username: String, copyData: Boolean): IO[CopyInfo] = {
    creatingCopy(as = username)(dataset, copyData) {
      copyInfo
    }.flatMap(finish(dataset))
  }
}
