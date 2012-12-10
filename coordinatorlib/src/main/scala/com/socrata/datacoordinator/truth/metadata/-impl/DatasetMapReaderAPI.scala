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

  trait IDatasetInfo extends com.socrata.datacoordinator.truth.metadata.DatasetInfo

  trait IVersionInfo extends com.socrata.datacoordinator.truth.metadata.VersionInfo {
    def tableInfo: DatasetInfo
  }

  trait IColumnInfo extends com.socrata.datacoordinator.truth.metadata.ColumnInfo {
    def versionInfo: VersionInfo
  }

  /** Looks up a dataset record by its ID. */
  def datasetInfo(datasetId: String): Option[DatasetInfo]

  /** Gets the newest copy, no matter what the lifecycle stage is. */
  def latest(datasetInfo: DatasetInfo): VersionInfo

  /** Returns the number of snapshots attached to this dataset. */
  def snapshotCount(datasetInfo: DatasetInfo): Int

  /** Loads the schema for the indicated dataset-version. */
  def schema(versionInfo: VersionInfo): Map[String, ColumnInfo]
}
