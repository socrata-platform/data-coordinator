package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.DatasetId

trait Secondary[CV] {
  def wantsWorkingCopies: Boolean

  /** The dataset has been deleted. */
  def dropDataset(datasetId: DatasetId)

  /**
   * @return The `dataVersion` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentVersion(datasetId: DatasetId): Long

  /**
   * @return The `copyNumber`s of all snapshot copies in this secondary.
   */
  def snapshots(datasetId: DatasetId): Set[Long]

  /**
   * Order this secondary to drop a snapshot.  This should ignore the request
   * if the snapshot is already gone (but it should signal an error if the
   * copyNumber does not name a snapshot).
   */
  def dropCopy(datasetId: DatasetId, copyNumber: Long)

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion. */
  def version(datasetId: DatasetId, dataVersion: Long, events: Iterator[Delogger.LogEvent[CV]])

  def resync(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo], rows: Managed[Iterator[Row[CV]]])
}
