package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata

case class SecondaryRecord(storeId: String, datasetId: DatasetId, startingDataVersion: Long, startingLifecycleStage: metadata.LifecycleStage, endingDataVersion: Long, initialCookie: Option[String])
class DatasetAlreadyInSecondary(val storeId: String, val DatasetId: DatasetId) extends Exception

trait SecondaryManifest {
  def readLastDatasetInfo(storeId: String, datasetId: DatasetId): Option[(Long, Option[String])]
  @throws(classOf[DatasetAlreadyInSecondary])
  def addDataset(storeId: String, datasetId: DatasetId)
  def dropDataset(storeId: String, datasetId: DatasetId)

  def datasets(storeId: String): Map[DatasetId, Long]
  def stores(datasetId: DatasetId): Map[String, Long]

  def findDatasetsNeedingReplication(storeId: String, limit: Int = 1000): Seq[SecondaryRecord]
  def completedReplicationTo(storeId: String, datasetId: DatasetId, dataVersion: Long, lifecycleStage: metadata.LifecycleStage, newCookie: Option[String])
}

case class NamedSecondary[CT, CV](storeId: String, store: Secondary[CT, CV])
