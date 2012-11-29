package com.socrata.datacoordinator.truth

import org.joda.time.DateTime

trait GlobalLog {
  /** Notes that a dataset has changed in some way.  The actual change log is in the dataset's private log table.
    * @note This may take an exclusive write-lock on a shared table.  THIS SHOULD BE THE VERY LAST THING
    *       A TRANSACTION DOES BEFORE IT COMMITS IN ORDER TO MINIMIZE THAT LOCK-TIME. */
  def log(datasetId: String, version: Long, updatedAt: DateTime, updatedBy: String)
}

trait DatasetMapReader {
  /** Looks up a Truthstore record by its ID */
  def tableInfo(datasetId: String): Option[TableInfo]

  /** Finds version information for this table's unpublished copy, if it has one. */
  def unpublished(table: TableInfo): Option[VersionInfo]

  /** Finds version information for this table's published copy, if it has one. */
  def published(table: TableInfo): Option[VersionInfo]

  /** Finds version information for this table's `age`th-oldest snapshotted copy, if it has one.
    * @param age 0 gets the newest snapshot, 1 the next newest, etc... */
  def snapshot(table: TableInfo, age: Int): Option[VersionInfo]

  /** Returns the number of snapshots attached to this table. */
  def snapshotCount(table: TableInfo): Int

  /** Gets the newest copy, no matter what the lifecycle stage is. */
  def latest(table: TableInfo): VersionInfo

  /** Loads the schema for the indicated table-version. */
  def schema(versionInfo: VersionInfo): Map[String, ColumnSpec]
}

trait DatasetMapLifecycleUpdater {
  /** Creates a new table in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping. */
  def create(datasetId: String, tableBase: String, userPrimaryKey: Option[String]): VersionInfo

  /** Completely removes a table (all its versions) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping. */
  def delete(datasetId: String)

  /** Delete this version of the table.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws IllegalArgumentException if the version does not name a snapshot or unpublished copy */
  def dropCopy(versionInfo: VersionInfo)

  /** Copies the "published" table record to an "unpublished" one.
    * @note Does not copy the actual tables; this just updates the bookkeeping. */
  def makeUnpublishedCopy(tableInfo: TableInfo): VersionInfo

  /** Promotes the current the "published" table record (if it exists) to an "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @return The version info for the newly-published dataset if there was an unpublished copy,
    *         or `None` if there wasn't. */
  def publish(tableInfo: TableInfo): Option[VersionInfo]
}

trait DatasetMapSchemaUpdater {
  /** Adds a column to this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnSpec

  /** Removes a column from this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def dropColumn(versionInfo: VersionInfo, logicalName: String)

  /** Changes the logical name of a column in this table-version. */
  def renameColumn(versionInfo: VersionInfo, oldLogicalName: String, newLogicalName: String)

  /** Changes the type and physical column base of a column in this table-version.
    * @note Does not change the actual table; this just updates the bookkeeping. */
  def convertColumn(versionInfo: VersionInfo, logicalName: String, newType: String, newPhysicalColumnBase: String)

  /** Changes the primary key column for this table-version.
    * @note Does not change the actual table (or verify it is a valid column to use as a PK); this just updates
    *       the bookkeeping. */
  def setUserPrimaryKey(versionInfo: VersionInfo, userPrimaryKey: Option[String])
}

case class ColumnSpec(VersionInfo: VersionInfo, logicalColumnName: String, typeName: String, physicalColumnBase: String)

case class TableInfo(datasetId: String, tableBase: String)

case class VersionInfo(tableInfo: TableInfo, logicalVersion: Long, lifecycleStage: LifecycleStage, userPrimaryKey: Option[String])
