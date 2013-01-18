package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.{ColumnId, VersionId, DatasetId}

trait DatasetMap extends `-impl`.BaseDatasetMap {
  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @note `datasetId` needs to be globally unique; if you have namespacing do it yourself.
    * @throws DatasetAlreadyExistsException if `datasetId` is already in use.
    * @return A `VersionInfo` that refers to an unpublished version. */
  def create(datasetId: String, tableBaseBase: String): VersionInfo

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @note This also updates the bookkeeping for columns.
    * @return Either the `VersionInfo` of an existing copy, or a pair of version
    *    infos for the version that was copied and the version it was copied to. */
  def ensureUnpublishedCopy(datasetInfo: DatasetInfo): Either[VersionInfo, CopyPair[VersionInfo]]

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `versionInfo` does not name an unpublished copy.
    * @return The version info for the newly-published dataset. */
  def publish(versionInfo: VersionInfo): VersionInfo

  /** Adds a column to this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column
    * @throws ColumnAlreadyExistsException if the column already exists */
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo
}

trait BackupDatasetMap extends `-impl`.BaseDatasetMap {
  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @note `datasetId` needs to be globally unique; if you have namespacing do it yourself.
    * @throws DatasetAlreadyExistsException if `datasetId` is already in use.
    * @throws DatasetSystemIdAlreadyInUse if `systemId` is already in use.
    * @return A `VersionInfo` that refers to an unpublished version with system id `systemId`. */
  def createWithId(systemId: DatasetId, datasetId: String, tableBaseBase: String, initialVersionSystemId: VersionId): VersionInfo

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @note This does NOT copy the schema, because those updates are sent separately.
    * @throws VersionSystemIdAlreadyInUse if `systemId` is already in use.
    * @return A pair of version infos for the version that was copied and the version it was copied to. */
  def createUnpublishedCopyWithId(datasetInfo: DatasetInfo, systemId: VersionId): CopyPair[VersionInfo]

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `versionInfo` does not name an unpublished copy.
    * @return The version info for the newly-published dataset. */
  def publish(versionInfo: VersionInfo): VersionInfo

  /** Adds a column to this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column
    * @throws ColumnAlreadyExistsException if the column already exists
    * @throws ColumnSystemIdAlreadyInUse if `systemId` already names a column on this version of the table. */
  def addColumnWithId(systemId: ColumnId, versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo
}

case class CopyPair[V <: VersionInfo](oldVersionInfo: V, newVersionInfo: V)
