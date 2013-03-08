package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.util.collection.DatasetIdMap

trait SecondaryManifest {
  def lastDataInfo(storeId: String, datasetId: DatasetId): (Long, Option[String])
  def updateDataInfo(storeId: String, datasetId: DatasetId, dataVersion: Long, cookie: Option[String])
  def dropDataset(storeId: String, datasetId: DatasetId)

  def datasets(storeId: String): DatasetIdMap[Long]
  def stores(datasetId: DatasetId): Map[String, Long]
}

case class NamedSecondary[CV](storeId: String, store: Secondary[CV])
