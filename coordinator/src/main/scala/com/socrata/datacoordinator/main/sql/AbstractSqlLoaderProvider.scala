package com.socrata.datacoordinator.main.sql

import java.sql.Connection
import java.util.concurrent.Executor

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, VersionInfo}
import com.socrata.datacoordinator.truth.loader.{RowPreparer, Loader, Logger}
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.truth.{TypeContext, RowIdMap, DatasetContext}
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.util.IdProviderPool
import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.main.soql.SystemColumns

abstract class AbstractSqlLoaderProvider[CT, CV](conn: Connection, idProviderPool: IdProviderPool, executor: Executor, typeContext: TypeContext[CT, CV])
  extends ((VersionInfo, Logger[CV]) => Managed[Loader[CV]])
{ self =>
  def produce(tableName: String, datasetContext: DatasetContext[CT, CV], repSchemaBuilder: ColumnIdMap[CT] => ColumnIdMap[SqlColumnRep[CT, CV]]): DataSqlizer[CT, CV]

  def apply(versionInfo: VersionInfo, schema: ColumnIdMap[ColumnInfo], rowPreparer: RowPreparer[CV], logger: Logger[CV], repFor: ColumnInfo => SqlColumnRep[CT, CV]) = {
    val tableName = versionInfo.dataTableName

    val repSchema = schema.mapValuesStrict(repFor)

    val userPrimaryKey = schema.iterator.find { case (_, colInfo) =>
      colInfo.isPrimaryKey
    }.map(_._1)

    val systemPrimaryKey = schema.iterator.find { case (_, colInfo) =>
      colInfo.logicalName == SystemColumns.id
    }.map(_._1).getOrElse { sys.error(s"No ${SystemColumns.id} column?") }

    val datasetContext = makeDatasetContext(versionInfo, repSchema, userPrimaryKey, systemPrimaryKey, schema.filter { (_, i) => i.logicalName.startsWith(":") }.keySet)

    // hrm, this seems like it should be done in a more straightforward
    // manner...
    def repSchemaBuilder(simpleSchema: ColumnIdMap[CT]): ColumnIdMap[SqlColumnRep[CT, CV]] =
      repSchema

    val sqlizer = produce(tableName, datasetContext, repSchemaBuilder)
    managed(SqlLoader(conn, rowPreparer, sqlizer, logger, idProviderPool, executor))
  }

  def makeDatasetContext(versionInfo: VersionInfo, rawSchema: ColumnIdMap[SqlColumnRep[CT, CV]], userPKCol: Option[ColumnId], idCol: ColumnId, systemIds: ColumnIdSet) =
    new RepBasedSqlDatasetContext[CT, CV] {
      val typeContext: TypeContext[CT, CV] = self.typeContext

      val schema = rawSchema

      val systemColumnIds = systemIds

      val systemIdColumn = idCol

      val userPrimaryKeyColumn: Option[ColumnId] = userPKCol

      def userPrimaryKey(row: Row[CV]): Option[CV] =
        row.get(userPrimaryKeyColumn.get)

      def systemId(row: Row[CV]): Option[RowId] =
        row.get(systemIdColumn).map(typeContext.makeSystemIdFromValue)

      def systemIdAsValue(row: Row[CV]): Option[CV] =
        row.get(systemIdColumn)

      def makeIdMap[T](): RowIdMap[CV, T] = ???

      def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV] = ???
    }
}

trait StandardSqlLoaderProvider[CT, CV] { this: AbstractSqlLoaderProvider[CT, CV] =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new StandardRepBasedDataSqlizer(tableName, datasetContext)
}

class PostgresSqlLoaderProvider[CT, CV] { this: AbstractSqlLoaderProvider[CT, CV] =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new PostgresRepBasedDataSqlizer(tableName, datasetContext)
}
