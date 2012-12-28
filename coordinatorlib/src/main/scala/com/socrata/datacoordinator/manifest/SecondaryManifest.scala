package com.socrata.datacoordinator
package manifest

import com.socrata.datacoordinator.util.collection.StoreIdMap
import com.socrata.datacoordinator.id.{DatasetId, StoreId}

trait SecondaryManifest {
  def create(storeId: StoreId, datasetId: DatasetId)
  def updateVersion(storeId: StoreId, datasetId: DatasetId, version: Long)
  def versionOf(storeId: StoreId, datasetId: DatasetId): Option[Long]
  def allVersionsOfDataset(dataset: DatasetId): StoreIdMap[Long]
  def remove(storeId: StoreId, datasetId: DatasetId)
}
