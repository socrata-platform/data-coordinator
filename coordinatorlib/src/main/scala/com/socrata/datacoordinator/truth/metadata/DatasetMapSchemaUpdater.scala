package com.socrata.datacoordinator.truth.metadata

trait DatasetMapSchemaUpdater {
  /** Adds a column to this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo

  /** Removes a column from this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return `true` if the column existed to be dropped. */
  def dropColumn(columnInfo: ColumnInfo): Boolean

  /** Changes the logical name of a column in this table-version.
    * @return The new column info if this column existed to be renamed. */
  def renameColumn(columnInfo: ColumnInfo, newLogicalName: String): Option[ColumnInfo]

  /** Changes the type and physical column base of a column in this table-version.
    * @note Does not change the actual table, or (if this column was a primary key) ensure that the new type is still
    *       a valid PK type; this just updates the bookkeeping.
    * @return The new column info if this column existed to be converted. */
  def convertColumn(columnInfo: ColumnInfo, newType: String, newPhysicalColumnBase: String): Option[ColumnInfo]

  /** Changes the primary key column for this table-version.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping.
    * @return The new column info if this column existed to be made primary.
    * @throws If there is a different primary key already defined. */
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo): Option[ColumnInfo]

  /** Clears the primary key column for this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return `true` if this version existed. */
  def clearUserPrimaryKey(versionInfo: VersionInfo): Boolean
}
