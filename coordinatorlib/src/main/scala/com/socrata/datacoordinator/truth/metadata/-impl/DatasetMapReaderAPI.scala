package com.socrata.datacoordinator.truth.metadata
package `-impl`

/** This shouldn't ever be referred to directly.  It exists to ensure that the
  * [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]] and
  * [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]] have the
  * same (but mutally incompatible) read-API. */
trait DatasetMapReaderAPI {
  /** Metadata for the entire dataset, such as the dataset ID and the base from which
    * physical table names are created.  Used to look up metadata for specific
    * dataset versions. */
  type DatasetInfo <: IDatasetInfo

  /** Metadata for a specific version of a dataset, such as the lifecycle stage.
    * Used to look up individual columns. */
  type VersionInfo <: IVersionInfo

  /** Metadata for a column of a specific version of a dataset, such as the column's
    * name and its type. */
  type ColumnInfo <: IColumnInfo

  trait IDatasetInfo {
    def datasetId: String
    def tableBase: String
  }

  trait IVersionInfo {
    def tableInfo: DatasetInfo
    def lifecycleVersion: Long
    def lifecycleStage: LifecycleStage
  }

  trait IColumnInfo {
    def versionInfo: VersionInfo
    def logicalName: String
    def typeName: String
    def isPrimaryKey: Boolean
  }

  /** Looks up a dataset record by its ID */
  def datasetInfo(datasetId: String): Option[DatasetInfo]

  /** Finds version information for this dataset's unpublished copy, if it has one. */
  def unpublished(table: DatasetInfo): Option[VersionInfo]

  /** Finds version information for this dataset's published copy, if it has one.
    * @note After a dataset is published for the first time, it always has a published
    *       version.*/
  def published(table: DatasetInfo): Option[VersionInfo]

  /** Finds version information for this dataset's `age`th-oldest snapshotted copy, if it has one.
    * @param age 0 gets the newest snapshot, 1 the next newest, etc... */
  def snapshot(table: DatasetInfo, age: Int): Option[VersionInfo]

  /** Returns the number of snapshots attached to this dataset. */
  def snapshotCount(table: DatasetInfo): Int

  /** Gets the newest copy, no matter what the lifecycle stage is. */
  def latest(table: DatasetInfo): VersionInfo

  /** Loads the schema for the indicated dataset-version. */
  def schema(versionInfo: VersionInfo): Map[String, ColumnInfo]
}
