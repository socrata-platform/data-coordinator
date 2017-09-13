package com.socrata.datacoordinator.secondary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait Secondary[CT, CV] {
  import Secondary.Cookie

  def shutdown(): Unit

  /** The dataset has been deleted. */
  def dropDataset(datasetInternalName: String, cookie: Cookie)

  /**
   * @return The `dataVersion` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentVersion(datasetInternalName: String, cookie: Cookie): Long

  /**
   * @return The `copyNumber` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Event[CT, CV]]): Cookie

  /**
   * Resyncs a copy of a dataset as part of the resync path.
   * This will only be called on copies that are published or unpublished.
   */
  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[CT]], cookie: Cookie,
             rows: Managed[Iterator[ColumnIdMap[CV]]], rollups: Seq[RollupInfo], isLatestLivingCopy: Boolean): Cookie

  /**
   * Drops a copy of a dataset as part of the resync path.
   * This will only be called on copies that are discarded or snapshotted.
   */
  def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Cookie, isLatestCopy: Boolean): Cookie

}

/** Thrown when a secondary decides it is not in sync and should be redone.  The process
  * propagating data to the secondary should catch it and immediately switch to `resync`.
  *
  * @note may be thrown from `resync` itself, though if it's done it probably means
  *       something is desperately wrong.
  */
case class ResyncSecondaryException(reason: String = "No reason") extends Exception(reason)

case class ResyncLaterSecondaryException(reason: String = "No reason") extends Exception(reason)

/**
 * Thrown when a secondary decides that it cannot do an update _right now_ .
 * The process propagating data to the secondary should try again later.
 */
case class ReplayLaterSecondaryException(reason: String = "No reason", cookie: Secondary.Cookie) extends Exception(reason)

case class BrokenDatasetSecondaryException(reason: String = "No reason", cookie: Secondary.Cookie) extends Exception(reason)

object Secondary {
  type Cookie = Option[String]
}
