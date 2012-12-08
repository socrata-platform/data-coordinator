package com.socrata.datacoordinator.truth.metadata

trait DatasetMapWriter extends `-impl`.DatasetMapReaderAPI {
  /** Looks up a dataset record by its ID.
    * @throws com.socrata.datacoordinator.truth.metadata.DatasetInUseByWriterException if another writer
    *                                                                                  is simultaneously trying
    *                                                                                  to access this dataset.
    */
  override def datasetInfo(datasetId: String): Option[DatasetInfo]

  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @note `datasetId` needs to be globally unique; if you have namespacing do it yourself.
    * @throws Exception if `datasetId` is already in use. TODO: better exception.
    * @return A `VersionInfo` that refers to an unpublished version. */
  def create(datasetId: String, tableBase: String): VersionInfo

  /** Completely removes a dataset (all its versions) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping. */
  def delete(datasetInfo: DatasetInfo)

  /** Delete this version of the dataset.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws IllegalArgumentException if the version does not name a snapshot or unpublished copy, or if
    *                                  the dataset has not yet been published for the first time. */
  def dropCopy(versionInfo: VersionInfo)

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @return A `VersionInfo` for an unpublished version. */
  def ensureUnpublishedCopy(datasetInfo: DatasetInfo): VersionInfo

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `versionInfo` does not name an unpublished copy.
    * @return The version info for the newly-published dataset if there was an unpublished copy,
    *         or `None` if there wasn't. */
  def publish(versionInfo: VersionInfo): VersionInfo

  /** Adds a column to this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column */
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo

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
}
