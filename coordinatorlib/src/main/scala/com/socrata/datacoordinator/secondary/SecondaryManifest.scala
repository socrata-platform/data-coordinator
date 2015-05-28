package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata
import scala.concurrent.duration.FiniteDuration
import java.util.UUID

case class SecondaryRecord(storeId: String,
                           claimantId: UUID,
                           datasetId: DatasetId,
                           startingDataVersion: Long,
                           startingLifecycleStage: metadata.LifecycleStage,
                           endingDataVersion: Long,
                           initialCookie: Option[String])

class DatasetAlreadyInSecondary(val storeId: String, val DatasetId: DatasetId) extends Exception

/**
 * Manages the manifest of replications to secondaries, persisted in a database.
 */
trait SecondaryManifest {
  def readLastDatasetInfo(storeId: String, datasetId: DatasetId): Option[(Long, Option[String])]
  @throws(classOf[DatasetAlreadyInSecondary])
  def addDataset(storeId: String, datasetId: DatasetId)
  def dropDataset(storeId: String, datasetId: DatasetId)

  def datasets(storeId: String): Map[DatasetId, Long]
  def stores(datasetId: DatasetId): Map[String, Long]

  def cleanOrphanedClaimedDatasets(storeId: String, claimantId: UUID)
  def claimDatasetNeedingReplication(storeId: String,
                                     claimantId: UUID,
                                     claimTimeout: FiniteDuration): Option[SecondaryRecord]
  def releaseClaimedDataset(job: SecondaryRecord)
  def markSecondaryDatasetBroken(job: SecondaryRecord)
  def completedReplicationTo(storeId: String,
                             claimantId: UUID,
                             datasetId: DatasetId,
                             dataVersion: Long,
                             lifecycleStage: metadata.LifecycleStage,
                             newCookie: Option[String])
}

case class NamedSecondary[CT, CV](storeId: String, store: Secondary[CT, CV])
