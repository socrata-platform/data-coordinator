package com.socrata.datacoordinator.truth.metadata
package `-impl`

import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.{ColumnName, TypeName}

trait BaseDatasetMapReader {
  /** Looks up a dataset record by its ID. */
  def datasetId(datasetId: String): Option[DatasetId]

  /** Gets the newest copy, no matter what the lifecycle stage is. */
  def latest(datasetInfo: DatasetInfo): CopyInfo

  def allCopies(datasetInfo: DatasetInfo): Iterable[CopyInfo]

  /** Returns the number of snapshots attached to this dataset. */
  def snapshotCount(datasetInfo: DatasetInfo): Int

  /** Loads the schema for the indicated dataset-copy. */
  def schema(CopyInfo: CopyInfo): ColumnIdMap[ColumnInfo]

  /** Finds information for this dataset's unpublished copy, if it has one. */
  def unpublished(datasetInfo: DatasetInfo): Option[CopyInfo]

  /** Finds information for this dataset's published copy, if it has one.
    * @note After a dataset is published for the first time, it always has a published
    *       copy.*/
  def published(datasetInfo: DatasetInfo): Option[CopyInfo]

  /** Finds information for this dataset's `age`th-oldest snapshotted copy, if it has one.
    * @param age 0 gets the newest snapshot, 1 the next newest, etc... */
  def snapshot(datasetInfo: DatasetInfo, age: Int): Option[CopyInfo]

  /** Finds information for the specified copy of this dataset, if it exists.
    * @param copyNumber The copy number to look up.
    */
  def copyNumber(datasetInfo: DatasetInfo, copyNumber: Long): Option[CopyInfo]
}

trait BaseDatasetMapWriter extends BaseDatasetMapReader {
  /** Completely removes a dataset (all its copies) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping. */
  def delete(datasetInfo: DatasetInfo)

  /** Delete this copy of the dataset.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws IllegalArgumentException if the copy does not name a snapshot or unpublished copy, or if
    *                                  the dataset has not yet been published for the first time. */
  def dropCopy(copyInfo: CopyInfo)

  /** Removes a column from this dataset-copy.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def dropColumn(columnInfo: ColumnInfo)

  /** Changes the logical name of a column in this dataset-copy.
    * @return The new column info. */
  def renameColumn(columnInfo: ColumnInfo, newLogicalName: ColumnName): ColumnInfo

  /** Changes the type and physical column base of a column in this dataset-copy.
    * @note Does not change the actual table, or (if this column was a primary key) ensure that the new type is still
    *       a valid PK type; this just updates the bookkeeping.
    * @return The new column info. */
  def convertColumn(columnInfo: ColumnInfo, newType: TypeName, newPhysicalColumnBaseBase: String): ColumnInfo

  /** Changes the system primary key column for this dataset-copy.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping.
    * @return The new column info.
    * @throws If there is a different primary key already defined. */
  def setSystemPrimaryKey(systemPrimaryKey: ColumnInfo): ColumnInfo

  /** Changes the user primary key column for this dataset-copy.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping.
    * @return The new column info.
    * @throws If there is a different primary key already defined. */
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo): ColumnInfo

  /** Clears the user primary key column for this dataset-copy.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def clearUserPrimaryKey(columnInfo: ColumnInfo): ColumnInfo

  /** Stores the next available row ID for this dataset. */
  def updateNextRowId(datasetInfo: DatasetInfo, newNextRowId: RowId): DatasetInfo

  /** Convenience overload which updates the provided `CopyInfo` with the
    * updated `DatasetInfo`. */
  def updateNextRowId(copyInfo: CopyInfo, newNextRowId: RowId): CopyInfo

  def updateDataVersion(copyInfo: CopyInfo, newDataVersion: Long): CopyInfo
}
