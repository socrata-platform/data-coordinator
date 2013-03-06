package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.DatasetId

trait SecondaryManifest {
  def lastDataInfo(datasetId: DatasetId): (Long, Option[String])
  def updateDataInfo(datasetId: DatasetId, dataVersion: Long, cookie: Option[String])
  def dropDataset(datasetId: DatasetId)
}

case class NamedSecondary[CV](storeId: String, store: Secondary[CV], manifest: SecondaryManifest)
