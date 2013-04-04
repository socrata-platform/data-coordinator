package com.socrata.datacoordinator.truth

import org.joda.time.DateTime
import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
import java.util.concurrent.ExecutorService
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.datacoordinator.id.RowId

@deprecated
trait DataTypeContext {
  type CT
  type CV

  /** The type of values which represent rows. */
  type Row = com.socrata.datacoordinator.Row[CV]

  /** The type of values which represent mutable rows. */
  type MutableRow = com.socrata.datacoordinator.MutableRow[CV]

  /** Methods to introspect type-related info. */
  val typeContext: TypeContext[CT, CV]

  /** Generator for a key used to obfuscate datasets' row IDs */
  val obfuscationKeyGenerator: () => Array[Byte]

  val initialRowId: RowId
}

@deprecated
trait ExecutionContext {
  val executorService: ExecutorService
  val timingReport: TimingReport
}

@deprecated
trait DataSchemaContext extends DataTypeContext {
  /** The set of all system columns, along with their types. */
  val systemColumns: Map[ColumnName, CT]

  /** The logical name of the system primary key column. */
  val systemIdColumnName: ColumnName

  /** Predicate to test whether a name belongs to a system column.
    * @note Just because this returns true, it is not necessarily a key in
    *       `systemColumns`.  It may simply be in some reserved namespace.
    */
  def isSystemColumn(name: ColumnName): Boolean

  /** Convenience alias for `isSystemColumn` which operates on [[com.socrata.datacoordinator.truth.metadata.ColumnInfo]]
    * and [[com.socrata.datacoordinator.truth.metadata.UnanchoredColumnInfo]] values.
    */
  def isSystemColumn(ci: ColumnInfoLike): Boolean = isSystemColumn(ci.logicalName)
}

@deprecated
trait DataWritingContext extends DataTypeContext {
  /** Creates a row preparer object for use within a series of insert or update events. */
  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[ColumnInfo]): RowPreparer[CV]

  /** Monad in which actions to update the database may occur. */
  val datasetMutator: DatasetMutator[CT, CV]

  /** An action that adds system columns to a dataset's schema. */
  def addSystemColumns(ctx: datasetMutator.MutationContext)

  /** Creates a codec for serializing or deserializing row data in the log table. */
  def newRowLogCodec(): RowLogCodec[CV]

  /** Utility method for creating values to use for the `physicalColumnBaseBase` parameter of
    * `datasetMutator.addColumn`. */
  def physicalColumnBaseBase(logicalColumnName: ColumnName, systemColumn: Boolean = false): String

  /** Predicate that tests whether the given identifier may be used as the `logical name` parameter
    * of `datasetMutator.addColumn`. */
  def isLegalLogicalName(identifier: ColumnName): Boolean // should this live in DataContext?

  val datasetMapLimits: DatasetMapLimits
}

@deprecated
trait DataReadingContext extends DataTypeContext {
  val datasetReader: Managed[DatasetReader[CV]]
}

@deprecated
trait CsvDataContext extends DataTypeContext {
  type CsvRepType = CsvColumnRep[CT, CV]

  def csvRepForColumn(typ: CT): CsvRepType
}
