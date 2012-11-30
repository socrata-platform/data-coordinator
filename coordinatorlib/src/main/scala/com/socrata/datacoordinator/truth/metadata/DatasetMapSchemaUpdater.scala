package com.socrata.datacoordinator.truth.metadata

trait DatasetMapSchemaUpdater {
  /** Adds a column to this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo

  /** Removes a column from this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def dropColumn(columnInfo: ColumnInfo)

  /** Changes the logical name of a column in this table-version. */
  def renameColumn(columnInfo: ColumnInfo, newLogicalName: String): ColumnInfo

  /** Changes the type and physical column base of a column in this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def convertColumn(columnInfo: ColumnInfo, newType: String, newPhysicalColumnBase: String): ColumnInfo

  /** Changes the primary key column for this table-version.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping. */
  def setUserPrimaryKey(versionInfo: VersionInfo, userPrimaryKey: Option[String])
}
