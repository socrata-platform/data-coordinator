package com.socrata.datacoordinator.truth.metadata
package `-impl`

import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait BaseDatasetMap {
  type DatasetInfo <: com.socrata.datacoordinator.truth.metadata.DatasetInfo
  type LocalVersion = { def datasetInfo: DatasetInfo }
  type VersionInfo <: com.socrata.datacoordinator.truth.metadata.VersionInfo with LocalVersion
  type LocalColumn = { def versionInfo: VersionInfo }
  type ColumnInfo <: com.socrata.datacoordinator.truth.metadata.ColumnInfo with LocalColumn

  /** Looks up a dataset record by its ID. */
  def datasetInfo(datasetId: String): Option[DatasetInfo]

  /** Looks up a dataset record by its system ID. */
  def datasetInfo(datasetId: DatasetId): Option[DatasetInfo]

  /** Gets the newest copy, no matter what the lifecycle stage is. */
  def latest(datasetInfo: DatasetInfo): VersionInfo

  /** Returns the number of snapshots attached to this dataset. */
  def snapshotCount(datasetInfo: DatasetInfo): Int

  /** Loads the schema for the indicated dataset-version. */
  def schema(versionInfo: VersionInfo): ColumnIdMap[ColumnInfo]

  /** Finds version information for this dataset's unpublished copy, if it has one. */
  def unpublished(datasetInfo: DatasetInfo): Option[VersionInfo]

  /** Finds version information for this dataset's published copy, if it has one.
    * @note After a dataset is published for the first time, it always has a published
    *       version.*/
  def published(datasetInfo: DatasetInfo): Option[VersionInfo]

  /** Finds version information for this dataset's `age`th-oldest snapshotted copy, if it has one.
    * @param age 0 gets the newest snapshot, 1 the next newest, etc... */
  def snapshot(datasetInfo: DatasetInfo, age: Int): Option[VersionInfo]

  /** Finds version information for the specified version of this dataset, if it exists.
    * @param lifecycleVersion The version to look up.
    * @note `lifecycleVersion` is not related to the "version" in the dataset's log.
    */
  def version(datasetInfo: DatasetInfo, lifecycleVersion: Long): Option[VersionInfo]

  /** Completely removes a dataset (all its versions) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping. */
  def delete(datasetInfo: DatasetInfo)

  /** Delete this version of the dataset.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws IllegalArgumentException if the version does not name a snapshot or unpublished copy, or if
    *                                  the dataset has not yet been published for the first time. */
  def dropCopy(versionInfo: VersionInfo)

  /** Removes a column from this dataset-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def dropColumn(columnInfo: ColumnInfo)

  /** Changes the logical name of a column in this dataset-version.
    * @return The new column info. */
  def renameColumn(columnInfo: ColumnInfo, newLogicalName: String): ColumnInfo

  /** Changes the type and physical column base of a column in this dataset-version.
    * @note Does not change the actual table, or (if this column was a primary key) ensure that the new type is still
    *       a valid PK type; this just updates the bookkeeping.
    * @return The new column info. */
  def convertColumn(columnInfo: ColumnInfo, newType: String, newPhysicalColumnBase: String): ColumnInfo

  /** Changes the primary key column for this dataset-version.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping.
    * @return The new column info.
    * @throws If there is a different primary key already defined. */
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo): ColumnInfo

  /** Clears the primary key column for this dataset-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def clearUserPrimaryKey(versionInfo: VersionInfo)

  /** Stores the next available row ID for this dataset. */
  def updateNextRowId(datasetInfo: DatasetInfo, newNextRowId: RowId): DatasetInfo
}
