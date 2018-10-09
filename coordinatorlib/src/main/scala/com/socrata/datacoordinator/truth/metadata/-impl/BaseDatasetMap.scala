package com.socrata.datacoordinator.truth.metadata
package `-impl`

import com.socrata.datacoordinator.id.{RollupName, DatasetId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

trait BaseDatasetMapReader[CT] {
  /** Gets the newest _live_ copy, no matter what the lifecycle stage is.
    * @note this will not return a discarded copy or a snapshot */
  def latest(datasetInfo: DatasetInfo): CopyInfo

  /** Returns all copies for this dataset, INCLUDING DISCARDED ONES.  The
    * results are guaranteed to be ordered by copy number. */
  def allCopies(datasetInfo: DatasetInfo): Iterable[CopyInfo]

  /** Returns the number of snapshots attached to this dataset. */
  def snapshotCount(datasetInfo: DatasetInfo): Int

  /** Loads the schema for the indicated dataset-copy. */
  def schema(CopyInfo: CopyInfo): ColumnIdMap[ColumnInfo[CT]]

  /** Finds information for this dataset's unpublished copy, if it has one. */
  def unpublished(datasetInfo: DatasetInfo): Option[CopyInfo]

  /** Finds information for this dataset's published copy, if it has one.
    * @note After a dataset is published for the first time, it always has a published
    *       copy.*/
  def published(datasetInfo: DatasetInfo): Option[CopyInfo]

  /** Finds information for this dataset's snapshotted copy with copy number `copy`, if it has one. */
  def snapshot(datasetInfo: DatasetInfo, copy: Long): Option[CopyInfo]

  /** Finds information for all this dataset's snapshots.  The results are guaranteed
    * to be ordered by copy number. */
  def snapshots(datasetInfo: DatasetInfo): Iterable[CopyInfo]

  /** Finds information for the specified copy of this dataset, if it exists.
    * @param copyNumber The copy number to look up.
    */
  def copyNumber(datasetInfo: DatasetInfo, copyNumber: Long): Option[CopyInfo]

  def allDatasetIds(): Seq[DatasetId]

  /** Returns all rollups for the given dataset copy.
   */
  def rollups(copyInfo: CopyInfo): Iterable[RollupInfo]

  /** Returns the RollupInfo for the specified rollup.
    */
  def rollup(copyInfo: CopyInfo, name: RollupName): Option[RollupInfo]

  /** Gets the current time.
   */
  def currentTime(): DateTime
}

trait BaseDatasetMapWriter[CT] extends BaseDatasetMapReader[CT] {
  /** Completely removes a dataset (all its copies) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping. */
  def delete(datasetInfo: DatasetInfo)

  /** Delete this copy of the dataset.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws CopyInWrongStateForDropException if the copy does not name a snapshot or unpublished copy
    * @throws CannotDropInitialWorkingCopyException if the dataset has not yet been published for the first time. */
  def dropCopy(copyInfo: CopyInfo)

  /**
    * Create a computation strategy from this column.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def addComputationStrategy(columnInfo: ColumnInfo[CT], computationStrategyInfo: ComputationStrategyInfo): ColumnInfo[CT]

  /**
    * Removes a computation strategy from this column.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def dropComputationStrategy(columnInfo: ColumnInfo[CT]): ColumnInfo[CT]

  /** Removes a column from this dataset-copy.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def dropColumn(columnInfo: ColumnInfo[CT])

  /**
   * Updates the field name of this dataset-copy
   * @note Does not change the actual table; this just updates the bookkeeping. */
  def updateFieldName(columnInfo: ColumnInfo[CT], newName: ColumnName): ColumnInfo[CT]

  /** Changes the system primary key column for this dataset-copy.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping.
    * @return The new column info.
    * @throws If there is a different primary key already defined. */
  def setSystemPrimaryKey(systemPrimaryKey: ColumnInfo[CT]): ColumnInfo[CT]

  /** Changes the version column for this dataset-copy.
    * @note Does not change the actual table (or verify it is a valid column to use as a version); this just updates
    *       the bookkeeping.
    * @return The new column info.
    * @throws If there is a different version column already defined. */
  def setVersion(version: ColumnInfo[CT]): ColumnInfo[CT]

  /** Changes the user primary key column for this dataset-copy.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping.
    * @return The new column info.
    * @throws If there is a different primary key already defined. */
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo[CT]): ColumnInfo[CT]

  /** Clears the user primary key column for this dataset-copy.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def clearUserPrimaryKey(columnInfo: ColumnInfo[CT]): ColumnInfo[CT]

  /** Stores the next available counter for this dataset. */
  def updateNextCounterValue(datasetInfo: DatasetInfo, newNextCounterValue: Long): DatasetInfo

  /** Convenience overload which updates the provided `CopyInfo` with the
    * updated `DatasetInfo`. */
  def updateNextCounterValue(copyInfo: CopyInfo, newNextCounterValue: Long): CopyInfo

  def updateDataVersion(copyInfo: CopyInfo, newDataVersion: Long): CopyInfo

  /** Updates the last-modified information for this copy.
    */
  def updateLastModified(copyInfo: CopyInfo, newLastModified: DateTime = currentTime()): CopyInfo

  /** Update the table modifier, producing a `CopyInfo` which refers to a new physical
    * database table.
    */
  def newTableModifier(copyInfo: CopyInfo): CopyInfo

  /** Creates or updates the metadata about the given rollup based on name and
    * given soql query.
    */
  def createOrUpdateRollup(copyInfo: CopyInfo, name: RollupName, soql: String): RollupInfo

  /** Drops the given rollup.
    */
  def dropRollup(copyInfo: CopyInfo, name: Option[RollupName])

}
