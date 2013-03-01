package com.socrata.datacoordinator.truth

import com.socrata.datacoordinator.id.DatasetId
import scala.concurrent.duration.Duration

trait DatasetLock {
  def withDatasetLock[T](datasetId: DatasetId, duration: Duration)(f: => T): T
}

object NoopDatasetLock extends DatasetLock {
  def withDatasetLock[T](datasetId: DatasetId, duration: Duration)(f: => T): T = f
}
