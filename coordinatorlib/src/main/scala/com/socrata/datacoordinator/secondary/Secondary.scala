package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.socrata.datacoordinator.id.DatasetId

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
  def snapshots(datasetInternalName: DatasetId, cookie: Cookie): Set[Long]

  /**
   * Order this secondary to drop a snapshot.  This should ignore the request
   * if the snapshot is already gone (but it should signal an error if the
   * copyNumber does not name a snapshot).
   */
  def dropCopy(datasetInternalName: DatasetId, copyNumber: Long, cookie: Cookie): Cookie

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  def version(datasetInternalName: DatasetId, dataVersion: Long, cookie: Cookie, events: Iterator[Delogger.LogEvent[CV]]): Cookie

  def resync(copyContext: DatasetCopyContext[CT], cookie: Cookie, rows: Managed[Iterator[Row[CV]]]): Cookie
}

object Secondary {
  type Cookie = Option[String]
}
