package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id._
import com.socrata.soql.environment.{ColumnName, ResourceName}
import scala.concurrent.duration.Duration

trait DatasetMapBase[CT] extends `-impl`.BaseDatasetMapReader[CT] {
}

trait DatasetMapReader[CT] extends DatasetMapBase[CT] {
  /** Looks up a dataset record by its system ID. */
  def datasetInfo(datasetId: DatasetId, repeatableRead: Boolean = false): Option[DatasetInfo]

  /** Looks up a dataset record by its resource name. */
  def datasetInfoByResourceName(resourceName: ResourceName, repeatableRead: Boolean = false): Option[DatasetInfo]

  /** Find all datasets with snapshots */
  def snapshottedDatasets(): Seq[DatasetInfo]

  /** Find all pre-simplified columns and their zoom levels. */
  def presimplifiedGeoColumns(copy: CopyInfo): Map[ColumnId, Seq[Int]]
}

class CopyInWrongStateForDropException(val copyInfo: CopyInfo, val acceptableStates: Set[LifecycleStage]) extends Exception
class CannotDropInitialWorkingCopyException(val copyInfo: CopyInfo) extends Exception

trait DatasetMapWriter[CT] extends DatasetMapBase[CT] with `-impl`.BaseDatasetMapWriter[CT] {
  /** Looks up a dataset record by its system ID.
    * @param timeout Amount of time to block before throwing.
    * @param semiExclusive A hint that this will not actually be doing writes to this row.
    * @note An implementation should make a "best effort" to honor the timeout, but
    *       is permitted to wait less or more.  In particular, the postgresql implementation
    *       will only wait up to `Int.MaxValue` milliseconds unless the timeout is
    *       actually non-finite.
    * @throws DatasetIdInUseByWriterException if some other writer has been used to look up this dataset. */
  def datasetInfo(datasetId: DatasetId, timeout: Duration, semiExclusive: Boolean = false): Option[DatasetInfo]

  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @return A `CopyInfo` that refers to an unpublished copy. */
  def create(localeName: String, resourceName: Option[String]): CopyInfo

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @note This also updates the bookkeeping for columns.
    * @note None of the new columns will be marked as being a primary key.
    * @return Either the `CopyInfo` of an existing copy, or a pair of CopyInfos
    *    for the copy that was duplicated and the new copy it was copied to. */
  def ensureUnpublishedCopy(datasetInfo: DatasetInfo): Either[CopyInfo, CopyPair[CopyInfo]]

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `copyInfo` does not name an unpublished copy.
    * @return The copy info for the newly-published dataset, and the copy info for the new snapshot if
    *         there was one. */
  def publish(copyInfo: CopyInfo): (CopyInfo, Option[CopyInfo])

  /** Adds a column to this table-copy.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column
    * @throws ColumnAlreadyExistsException if the column already exists */
  def addColumn(copyInfo: CopyInfo, userColumnId: UserColumnId, fieldName: Option[ColumnName], typ: CT, physicalColumnBaseBase: String, computationStrategyInfo: Option[ComputationStrategyInfo]): ColumnInfo[CT]
}

trait BackupDatasetMap[CT] extends DatasetMapWriter[CT] with `-impl`.BaseDatasetMapWriter[CT] {
  /** Creates a new dataset in the truthstore.
    * @note Does not actually create any tables; this just updates the bookkeeping.
    * @throws DatasetSystemIdAlreadyInUse if `systemId` is already in use.
    * @return A `CopyInfo` that refers to an unpublished copy with system id `systemId`. */
  def createWithId(systemId: DatasetId, initialCopySystemId: CopyId, localeName: String, obfuscationKey: Array[Byte], resourceName: Option[String]): CopyInfo

  /** Ensures that an "unpublished" table exists, creating it if necessary.
    * @note Does not copy the actual tables; this just updates the bookkeeping.
    * @note This does NOT copy the schema, because those updates are sent separately.
    * @throws CopySystemIdAlreadyInUse if `systemId` is already in use.
    * @return A pair of copy infos for the copy that was copied and the copy it was copied to. */
  def createUnpublishedCopyWithId(datasetInfo: DatasetInfo, systemId: CopyId): CopyPair[CopyInfo]

  /** Promotes the current "published" table record (if it exists) to a "snapshot" one, and promotes the
    * current "unpublished" table record to "published".
    * @throws IllegalArgumentException if `copyInfo` does not name an unpublished copy.
    * @return The copy info for the newly-published dataset. */
  def publish(copyInfo: CopyInfo): (CopyInfo, Option[CopyInfo])

  /** Adds a column to this table-copy.
    * @note Does not change the actual table; this just updates the bookkeeping.
    * @return The new column
    * @throws ColumnAlreadyExistsException if the column already exists
    * @throws ColumnSystemIdAlreadyInUse if `systemId` already names a column on this copy of the table. */
  def addColumnWithId(systemId: ColumnId, copyInfo: CopyInfo, userColumnId: UserColumnId, fieldName: Option[ColumnName], typ: CT, physicalColumnBaseBase: String, computationStrategyInfo: Option[ComputationStrategyInfo]): ColumnInfo[CT]

  /** Creates a dataset with the specified attributes
    * @note Using this carelessly can get you into trouble.  In particular, this
    *       newly created dataset will have NO copies attached. */
  def unsafeCreateDataset(systemId: DatasetId,
                          nextCounterValue: Long,
                          localeName: String,
                          obfuscationKey: Array[Byte],
                          resourceName: Option[String]): DatasetInfo

  /** Creates a dataset with the specified attributes
    * @note Using this carelessly can get you into trouble.  In particular, this
    *       newly created dataset will have NO copies attached. */
  def unsafeCreateDatasetAllocatingSystemId(localeName: String,
                                            obfuscationKey: Array[Byte],
                                            resourceName: Option[String]): DatasetInfo

  /** Reloads a dataset with the specified attributes, including CLEARING ALL COPIES.
    * @note Using this carelessly can get you into trouble.  It is intended to be used
    *       for resyncing only.  The resulting dataset object will have NO copies. */
  def unsafeReloadDataset(datasetInfo: DatasetInfo,
                          nextCounterValue: Long,
                          localeName: String,
                          obfuscationKey: Array[Byte],
                          resourceName: Option[String]): DatasetInfo

  /** Creates a copy with the specified attributes.
    * @note Using this carelessly can get you into trouble.  It is intended to be used
    *       for resyncing only. */
  def unsafeCreateCopy(datasetInfo: DatasetInfo,
                       systemId: CopyId,
                       copyNumber: Long,
                       lifecycleStage: LifecycleStage,
                       dataVersion: Long): CopyInfo
}

case class CopyPair[A <: CopyInfo](oldCopyInfo: A, newCopyInfo: A)
