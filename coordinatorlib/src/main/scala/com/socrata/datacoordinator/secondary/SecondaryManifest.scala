package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.DatasetId

trait SecondaryManifest {
  def readLastDatasetInfo(storeId: String, datasetId: DatasetId): Option[(Long, Option[String])]
  def lastDataInfo(storeId: String, datasetId: DatasetId): (Long, Option[String])
  def updateDataInfo(storeId: String, datasetId: DatasetId, dataVersion: Long, cookie: Option[String])
  def dropDataset(storeId: String, datasetId: DatasetId)

  def datasets(storeId: String): Map[DatasetId, Long]
  def stores(datasetId: DatasetId): Map[String, Long]
}

case class NamedSecondary[CT, CV](storeId: String, store: Secondary[CT, CV])
