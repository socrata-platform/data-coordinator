package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.id.DatasetId

trait DatasetDropper {
  def dropDataset(datasetId: DatasetId): DatasetDropper.Result
}

object DatasetDropper {
  sealed abstract class Result
  case object Success extends Result
  sealed abstract class Failure extends Result
  case object FailureNotFound extends Failure
  case object FailureWriteLock extends Failure

}
