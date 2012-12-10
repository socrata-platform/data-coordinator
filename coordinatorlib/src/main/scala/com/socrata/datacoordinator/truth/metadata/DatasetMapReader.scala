package com.socrata.datacoordinator.truth.metadata

trait DatasetMapReader extends `-impl`.DatasetMapReaderAPI {
  /** Finds version information for this dataset's unpublished copy, if it has one. */
  def unpublished(datasetInfo: DatasetInfo): Option[VersionInfo]

  /** Finds version information for this dataset's published copy, if it has one.
    * @note After a dataset is published for the first time, it always has a published
    *       version.*/
  def published(datasetInfo: DatasetInfo): Option[VersionInfo]

  /** Finds version information for this dataset's `age`th-oldest snapshotted copy, if it has one.
    * @param age 0 gets the newest snapshot, 1 the next newest, etc... */
  def snapshot(datasetInfo: DatasetInfo, age: Int): Option[VersionInfo]

  /** Finds version information for the specified version of this dataset, if it exists.
    * @param lifecycleVersion The version to look up.
    * @note `lifecycleVersion` is not related to the "version" in the dataset's log.
    */
  def version(datasetInfo: DatasetInfo, lifecycleVersion: Long): Option[VersionInfo]
}
