package com.socrata.datacoordinator.truth.metadata

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
  def schema(versionInfo: VersionInfo): Map[String, ColumnInfo]
}
