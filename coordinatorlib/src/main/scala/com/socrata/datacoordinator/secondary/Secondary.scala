package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.socrata.datacoordinator.id.DatasetId

trait SecondaryDatasetInfo {
  val internalName: String
  val obfuscationKey: Array[Byte]
}

trait Secondary[CT, CV] {
  import Secondary.Cookie

  def shutdown()

  def wantsWorkingCopies: Boolean

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

  /**
   * @return The `copyNumber`s of all snapshot copies in this secondary.
   */
  def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long]

  /**
   * Order this secondary to drop a snapshot.  This should ignore the request
   * if the snapshot is already gone (but it should signal an error if the
   * copyNumber does not name a snapshot).
   */
  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  def version(datasetInfo: SecondaryDatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Delogger.LogEvent[CV]]): Cookie

  def resync(datasetInfo: SecondaryDatasetInfo, copyContext: DatasetCopyContext[CT], cookie: Cookie, rows: Managed[Iterator[Row[CV]]]): Cookie
}

/** Thrown when a secondary decides it is not in sync and should be redone.  The process
  * propagating data to the secondary should catch it and immediately switch to `resync`.
  *
  * @note may be thrown from `resync` itself, though if it's done it probably means
  *       something is desperately wrong.
  */
case class ResyncSecondaryException(reason: String = "No reason") extends Exception(reason)

object Secondary {
  type Cookie = Option[String]
}
