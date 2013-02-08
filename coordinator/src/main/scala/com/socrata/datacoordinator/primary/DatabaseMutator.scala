package com.socrata.datacoordinator.primary

import org.joda.time.DateTime
import com.rojoma.simplearm.Managed

import com.socrata.id.numeric.IdProvider

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.ColumnIdMap

abstract class DatabaseMutator[CT, CV] {
  trait ProviderOfNecessaryThings {
    val now: DateTime
    val datasetMap: DatasetMapWriter
    def datasetLog(ds: DatasetInfo): Logger[CV]
    def delogger(ds: DatasetInfo): Delogger[CV]
    val globalLog: GlobalLog
    def physicalColumnBaseForType(typ: CT): String
    def schemaLoader(version: CopyInfo, logger: Logger[CV]): SchemaLoader
    def nameForType(typ: CT): String

    def dataLoader(table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV], dataIdProvider: IdProvider): Managed[Loader[CV]]
    def rowPreparer(schema: ColumnIdMap[ColumnInfo]): RowPreparer[CV]
  }

  trait BaseUpdate {
    val now: DateTime
    val datasetMap: DatasetMapWriter
    val initialDatasetInfo: datasetMap.DatasetInfo
    val initialCopyInfo: datasetMap.CopyInfo
    val datasetLog: Logger[CV]
  }

  trait SchemaUpdate extends BaseUpdate {
    val schemaLoader: SchemaLoader
    def datasetContentsCopier: DatasetContentsCopier
  }

  trait DataUpdate extends BaseUpdate {
    val initialSchema: ColumnIdMap[datasetMap.ColumnInfo]
    val dataLoader: Loader[CV]
  }

  def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T
  def withSchemaUpdate[T](datasetId: String, user: String)(f: SchemaUpdate => T): T
  def withDataUpdate[T](datasetId: String, user: String)(f: DataUpdate => T): T
}
