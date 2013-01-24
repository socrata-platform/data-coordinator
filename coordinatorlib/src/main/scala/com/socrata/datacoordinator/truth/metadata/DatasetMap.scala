package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.{ColumnId, CopyId, DatasetId}

trait DatasetMap extends `-impl`.BaseDatasetMap {
  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @note `datasetId` needs to be globally unique; if you have namespacing do it yourself.
    * @throws DatasetAlreadyExistsException if `datasetId` is already in use.
    * @return A `CopyInfo` that refers to an unpublished copy. */
  def create(datasetId: String, tableBaseBase: String): CopyInfo

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @note This also updates the bookkeeping for columns.
    * @return Either the `CopyInfo` of an existing copy, or a pair of CopyInfos
    *    for the copy that was duplicated and the new copy it was copied to. */
  def ensureUnpublishedCopy(datasetInfo: DatasetInfo): Either[CopyInfo, CopyPair[CopyInfo]]

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `copyInfo` does not name an unpublished copy.
    * @return The copy info for the newly-published dataset. */
  def publish(copyInfo: CopyInfo): CopyInfo

  /** Adds a column to this table-copy.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column
    * @throws ColumnAlreadyExistsException if the column already exists */
  def addColumn(copyInfo: CopyInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo
}

trait BackupDatasetMap extends `-impl`.BaseDatasetMap {
  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @note `datasetId` needs to be globally unique; if you have namespacing do it yourself.
    * @throws DatasetAlreadyExistsException if `datasetId` is already in use.
    * @throws DatasetSystemIdAlreadyInUse if `systemId` is already in use.
    * @return A `CopyInfo` that refers to an unpublished copy with system id `systemId`. */
  def createWithId(systemId: DatasetId, datasetId: String, tableBaseBase: String, initialCopySystemId: CopyId): CopyInfo

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @note This does NOT copy the schema, because those updates are sent separately.
    * @throws CopySystemIdAlreadyInUse if `systemId` is already in use.
    * @return A pair of copy infos for the copy that was copied and the copy it was copied to. */
  def createUnpublishedCopyWithId(datasetInfo: DatasetInfo, systemId: CopyId): CopyPair[CopyInfo]

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `copyInfo` does not name an unpublished copy.
    * @return The copy info for the newly-published dataset. */
  def publish(copyInfo: CopyInfo): CopyInfo

  /** Adds a column to this table-copy.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column
    * @throws ColumnAlreadyExistsException if the column already exists
    * @throws ColumnSystemIdAlreadyInUse if `systemId` already names a column on this copy of the table. */
  def addColumnWithId(systemId: ColumnId, copyInfo: CopyInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo
}

case class CopyPair[A <: CopyInfo](oldCopyInfo: A, newCopyInfo: A)
