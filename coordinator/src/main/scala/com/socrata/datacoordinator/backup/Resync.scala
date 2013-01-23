package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, VersionInfo, DatasetInfo}

object Resync {
  def apply[T](dataset: T, explanation: String)(implicit ev: DatasetIdable[T]): Nothing =
    throw new Exception("Dataset " + ev.datasetId(dataset) + ": " + explanation)

  @inline final def unless[T: DatasetIdable](dataset: T, condition: Boolean, explanation: => String) {
    if(!condition) apply(dataset, explanation)
  }
}

trait DatasetIdable[-T] { def datasetId(x: T): DatasetId }
object DatasetIdable {
  implicit object datasetIdableDatasetId extends DatasetIdable[DatasetId] { def datasetId(x: DatasetId) = x }
  implicit object datasetIdableDatasetInfo extends DatasetIdable[DatasetInfo] { def datasetId(x: DatasetInfo) = x.systemId }
  implicit object datasetIdableVersionInfo extends DatasetIdable[VersionInfo] { def datasetId(x: VersionInfo) = x.datasetInfo.systemId }
  implicit object datasetIdableColumnInfo extends DatasetIdable[ColumnInfo] { def datasetId(x: ColumnInfo) = x.versionInfo.datasetInfo.systemId }
}

