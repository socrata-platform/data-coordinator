package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.id.DatasetId

trait DatasetRemover {
  def removeDataset(datasetId: DatasetId): DatasetRemover.Result
}

object DatasetRemover {
  sealed abstract class Result
  case object Success extends Result
  sealed abstract class Failure extends Result
  case object FailureNotFound extends Failure
  case object FailureWriteLock extends Failure

}
