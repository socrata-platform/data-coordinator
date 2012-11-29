package com.socrata.datacoordinator.truth.metadata

trait DatasetMapLifecycleUpdater {
  /** Creates a new table in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping. */
  def create(datasetId: String, tableBase: String, userPrimaryKey: Option[String]): VersionInfo

  /** Completely removes a table (all its versions) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping. */
  def delete(tableInfo: TableInfo): Boolean

  /** Delete this version of the table.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws IllegalArgumentException if the version does not name a snapshot or unpublished copy */
  def dropCopy(versionInfo: VersionInfo): Boolean

  /** Copies the "published" table record to an "unpublished" one.
    * @note Does not copy the actual tables; this just updates the bookkeeping. */
  def ensureUnpublishedCopy(tableInfo: TableInfo): VersionInfo

  /** Promotes the current the "published" table record (if it exists) to an "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @return The version info for the newly-published dataset if there was an unpublished copy,
    *         or `None` if there wasn't. */
  def publish(tableInfo: TableInfo): Option[VersionInfo]
}
