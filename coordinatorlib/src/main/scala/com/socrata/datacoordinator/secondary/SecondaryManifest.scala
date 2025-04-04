package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.truth.metadata

import scala.concurrent.duration.FiniteDuration
import java.util.UUID

import org.joda.time.DateTime

case class SecondaryRecord(storeId: String,
                           claimantId: UUID,
                           datasetId: DatasetId,
                           startingDataVersion: Long,
                           endingDataVersion: Long,
                           retryNum: Int,
                           replayNum: Int,
                           initialCookie: Option[String],
                           pendingDrop: Boolean,
                           nextRetry: DateTime)

class DatasetAlreadyInSecondary(val storeId: String, val DatasetId: DatasetId) extends Exception

// Secondary.  Secondary.  Secondary.  Secondary.  Secondary.
// Secondary.  Secondary.  Secondary.  Secondary.  Secondary.
// Secondary.  Secondary.  Secondary.  Secondary.  Secondary.
case class BrokenSecondaryRecord(
  storeId: String,
  datasetId: DatasetId,
  brokenAt: DateTime,
  brokenAcknowledgedAt: Option[DateTime],
  retryNum: Long,
  replayNum: Long
)

/**
 * Manages the manifest of replications to secondaries, persisted in a database.
 */
trait SecondaryManifest {
  def readLastDatasetInfo(storeId: String, datasetId: DatasetId): Option[(Long, Option[String])]
  @throws(classOf[DatasetAlreadyInSecondary])
  def addDataset(storeId: String, datasetId: DatasetId): Unit
  def dropDataset(storeId: String, datasetId: DatasetId): Unit

  def datasets(storeId: String): Map[DatasetId, Long]
  def stores(datasetId: DatasetId): Map[String, SecondaryManifest.StoreInfo]
  def brokenAts(datasetId: DatasetId): Map[String, DateTime]

  def allBrokenDatasets: Map[String, Map[DatasetId, BrokenSecondaryRecord]]
  def unbreakDataset(storeId: String, datasetId: DatasetId): Boolean
  def acknowledgeBroken(storeId: String, datasetId: DatasetId): Boolean

  def cleanOrphanedClaimedDatasets(storeId: String, claimantId: UUID): Unit
  def claimDatasetNeedingReplication(storeId: String,
                                     claimantId: UUID,
                                     claimTimeout: FiniteDuration): Option[SecondaryRecord]
  def releaseClaimedDataset(job: SecondaryRecord): Unit
  def markSecondaryDatasetBroken(job: SecondaryRecord, cookie: Cookie): Unit
  def completedReplicationTo(storeId: String,
                             claimantId: UUID,
                             datasetId: DatasetId,
                             dataVersion: Long,
                             newCookie: Option[String])
  def updateRetryInfo(storeId: String, datasetId: DatasetId, retryNum: Int, nextRetryDelaySecs: Int): Unit

  def updateReplayInfo(storeId: String, datasetId: DatasetId, cookie: Secondary.Cookie, replayNum: Int, nextReplayDelaySecs: Int): Unit

  def markDatasetForDrop(storeId: String, datasetId: DatasetId): Boolean

  def feedbackSecondaries(datasetId: DatasetId): Set[String] // store IDs
  def outOfDateFeedbackSecondaries(datasetId: DatasetId): Set[String] // store IDs

  def isInSecondary(datasetId: DatasetId, storeId: String): Boolean

  def performResync(datasetId: DatasetId, storeId: String): Unit
  def lockResync(datasetId: DatasetId, storeId: String, groupName: String): Unit
  def unlockResync(datasetId: DatasetId, storeId: String, groupName: String): Unit
}

object SecondaryManifest {
  case class StoreInfo(
    latestSecondaryDataVersion: Long,
    pendingDrop: Boolean,
    brokenAt: Option[DateTime],
    isFeedback: Boolean,
    secondaryGroup: Option[String],
  )
}

case class NamedSecondary[CT, CV](storeId: String, store: Secondary[CT, CV], groupName: String)
