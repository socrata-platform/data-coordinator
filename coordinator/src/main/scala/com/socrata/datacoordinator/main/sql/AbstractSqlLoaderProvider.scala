package com.socrata.datacoordinator.main.sql

import java.sql.Connection
import java.util.concurrent.ExecutorService

import com.socrata.id.numeric.IdProvider

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, VersionInfo}
import com.socrata.datacoordinator.truth.loader.{RowPreparer, Loader, Logger}
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.truth.TypeContext
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.main.soql.SystemColumns

abstract class AbstractSqlLoaderProvider[CT, CV](conn: Connection, idProvider: IdProvider, val executor: ExecutorService, typeContext: TypeContext[CT, CV])
  extends ((VersionInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], Logger[CV], ColumnInfo => SqlColumnRep[CT, CV]) => Loader[CV])
{ self =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]): DataSqlizer[CT, CV]

  def apply(versionInfo: VersionInfo, schema: ColumnIdMap[ColumnInfo], rowPreparer: RowPreparer[CV], logger: Logger[CV], repFor: ColumnInfo => SqlColumnRep[CT, CV]) = {
    val tableName = versionInfo.dataTableName

    val repSchema = schema.mapValuesStrict(repFor)

    val userPrimaryKeyInfo = schema.values.iterator.find(_.isUserPrimaryKey).map { colInfo =>
      (colInfo.systemId, repSchema(colInfo.systemId).representedType)
    }

    val systemPrimaryKey = schema.iterator.find { case (_, colInfo) =>
      colInfo.logicalName == SystemColumns.id
    }.map(_._1).getOrElse { sys.error(s"No ${SystemColumns.id} column?") }

    val datasetContext = makeDatasetContext(versionInfo, repSchema, userPrimaryKeyInfo, systemPrimaryKey, schema.filter { (_, i) => i.logicalName.startsWith(":") }.keySet)

    val sqlizer = produce(tableName, datasetContext)
    SqlLoader(conn, rowPreparer, sqlizer, logger, idProvider, executor)
  }

  def makeDatasetContext(versionInfo: VersionInfo, rawSchema: ColumnIdMap[SqlColumnRep[CT, CV]], userPKCol: Option[(ColumnId, CT)], idCol: ColumnId, systemIds: ColumnIdSet) =
    new RepBasedSqlDatasetContext[CT, CV] {
      val typeContext: TypeContext[CT, CV] = self.typeContext

      val schema = rawSchema

      val systemColumnIds = systemIds

      val systemIdColumn = idCol

      val userPrimaryKeyColumn: Option[ColumnId] = userPKCol.map(_._1)
      val userPrimaryKeyType: Option[CT] = userPKCol.map(_._2)

      def userPrimaryKey(row: Row[CV]): Option[CV] =
        row.get(userPrimaryKeyColumn.get)

      def systemId(row: Row[CV]): Option[RowId] =
        row.get(systemIdColumn).map(typeContext.makeSystemIdFromValue)

      def systemIdAsValue(row: Row[CV]): Option[CV] =
        row.get(systemIdColumn)

      def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV] = base ++ overlay
    }
}

trait StandardSqlLoaderProvider[CT, CV] { this: AbstractSqlLoaderProvider[CT, CV] =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new StandardRepBasedDataSqlizer(tableName, datasetContext)
}

trait PostgresSqlLoaderProvider[CT, CV] { this: AbstractSqlLoaderProvider[CT, CV] =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new PostgresRepBasedDataSqlizer(tableName, datasetContext, executor)
}
