package com.socrata.datacoordinator.truth

import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.metadata.{ColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
import java.util.concurrent.ExecutorService
import com.socrata.datacoordinator.truth.csv.CsvColumnRep

trait DataTypeContext {
  type CT
  type CV

  /** The type of values which represent rows. */
  type Row = com.socrata.datacoordinator.Row[CV]

  /** The type of values which represent mutable rows. */
  type MutableRow = com.socrata.datacoordinator.MutableRow[CV]

  /** Methods to introspect type-related info. */
  val typeContext: TypeContext[CT, CV]
}

trait ExecutionContext {
  val executorService: ExecutorService
}

trait DataSchemaContext extends DataTypeContext {
  /** The set of all system columns, along with their types. */
  val systemColumns: Map[String, CT]

  /** The logical name of the system primary key column. */
  val systemIdColumnName: String

  /** Predicate to test whether a name belongs to a system column.
    * @note Just because this returns true, it is not necessarily a key in
    *       `systemColumns`.  It may simply be in some reserved namespace.
    */
  def isSystemColumn(name: String): Boolean

  /** Convenience alias for `isSystemColumn` which operates on [[com.socrata.datacoordinator.truth.metadata.ColumnInfo]]
    * and [[com.socrata.datacoordinator.truth.metadata.UnanchoredColumnInfo]] values.
    */
  def isSystemColumn(ci: ColumnInfoLike): Boolean = isSystemColumn(ci.logicalName)
}

trait DataWritingContext extends DataTypeContext {
  /** Creates a row preparer object for use within a series of insert or update events. */
  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[ColumnInfo]): RowPreparer[CV]

  /** Monad in which actions to update the database may occur. */
  val datasetMutator: DatasetMutator[CV]

  /** An action that adds system columns to a dataset's schema. */
  def addSystemColumns(ctx: datasetMutator.MutationContext)

  /** Creates a codec for serializing or deserializing row data in the log table. */
  def newRowLogCodec(): RowLogCodec[CV]

  /** Utility method for creating values to use for the `physicalColumnBaseBase` parameter of
    * `datasetMutator.addColumn`. */
  def physicalColumnBaseBase(logicalColumnName: String, systemColumn: Boolean = false): String

  /** Predicate that tests whether the given identifier may be used as the `logical name` parameter
    * of `datasetMutator.addColumn`. */
  def isLegalLogicalName(identifier: String): Boolean // should this live in DataContext?

  val datasetMapLimits: DatasetMapLimits
}

trait DataReadingContext extends DataTypeContext {
  val datasetReader: DatasetReader[CV]
}

trait CsvDataContext extends DataTypeContext {
  type CsvRepType = CsvColumnRep[CT, CV]

  def csvRepForColumn(typ: CT): CsvRepType
}

trait JsonDataTypeContext extends DataTypeContext {
  type JsonRepType <: json.JsonColumnCommonRep[CT, CV]
  def jsonRepForColumn(name: String, typ: CT): JsonRepType
  final def jsonRepForColumn(ci: ColumnInfo): JsonRepType = jsonRepForColumn(ci.logicalName, typeContext.typeFromName(ci.typeName))
}

trait JsonDataReadingContext extends JsonDataTypeContext {
  type JsonRepType <: json.JsonColumnReadRep[CT, CV]
}

trait JsonDataWritingContext extends JsonDataTypeContext {
  type JsonRepType <: json.JsonColumnWriteRep[CT, CV]
}

trait JsonDataContext extends JsonDataReadingContext with JsonDataWritingContext {
  type JsonRepType = json.JsonColumnRep[CT, CV]
}
