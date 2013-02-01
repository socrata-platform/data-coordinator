package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, DatasetInfo}

object Resync {
  def apply[T](dataset: T, explanation: String)(implicit ev: DatasetIdable[T]): Nothing =
    throw new Exception("Dataset " + ev.datasetId(dataset).underlying + ": " + explanation)

  @inline final def unless[T: DatasetIdable](dataset: T, condition: Boolean, explanation: => String) {
    if(!condition) apply(dataset, explanation)
  }
}
