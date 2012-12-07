package com.socrata.datacoordinator.truth.metadata

trait DatasetMapLifecycleUpdater {
  /** Creates a new table in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @note `datasetId` needs to be globally unique; if you have namespacing do it yourself.
    * @throws Exception if `datasetId` is already in use. TODO: better exception.
    * @return A [[com.socrata.datacoordinator.truth.metadata.VersionInfo]] that refers to an unpublished version. */
  def create(datasetId: String, tableBase: String): VersionInfo

  /** Completely removes a table (all its versions) from the truthstore.
    * @note Does not actually drop (or queue for dropping) any tables; this just updates the bookkeeping.
    * @return `true` if `tableInfo` referred to a table that existed, otherwise `false`. */
  def delete(tableInfo: TableInfo): Boolean

  /** Delete this version of the table.
    * @note Does not drop the actual tables or even queue them for dropping; this just updates the bookkeeping.
    * @throws IllegalArgumentException if the version does not name a snapshot or unpublished copy, or if
    *                                  the dataset has not yet been published for the first time.
    * @return `true` if `versionInfo` referred to a version that existed, with the correct lifecycle version,
    *        otherwise false. */
  def dropCopy(versionInfo: VersionInfo): Boolean

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @return A [[com.socrata.datacoordinator.truth.metadata.VersionInfo]] for an unpublished version if this
    *         table existed at all. */
  def ensureUnpublishedCopy(tableInfo: TableInfo): Option[VersionInfo]

  /** Promotes the current the "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @return The version info for the newly-published dataset if there was an unpublished copy,
    *         or `None` if there wasn't. */
  def publish(tableInfo: TableInfo): Option[VersionInfo]
}
