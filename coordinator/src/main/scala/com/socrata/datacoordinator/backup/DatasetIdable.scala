package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, DatasetInfo}

trait DatasetIdable[-T] { def datasetId(x: T): DatasetId }
object DatasetIdable {
  implicit object datasetIdableDatasetId extends DatasetIdable[DatasetId] { def datasetId(x: DatasetId) = x }
  implicit object datasetIdableDatasetInfo extends DatasetIdable[DatasetInfo] { def datasetId(x: DatasetInfo) = x.systemId }
  implicit object datasetIdableVersionInfo extends DatasetIdable[CopyInfo] { def datasetId(x: CopyInfo) = x.datasetInfo.systemId }
  implicit object datasetIdableColumnInfo extends DatasetIdable[ColumnInfo[_]] { def datasetId(x: ColumnInfo[_]) = x.copyInfo.datasetInfo.systemId }
}
