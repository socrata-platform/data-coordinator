package com.socrata.datacoordinator.common.sql

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
import com.socrata.datacoordinator.common.soql.SystemColumns

abstract class AbstractSqlLoaderProvider[CT, CV](conn: Connection, idProvider: IdProvider, val executor: ExecutorService, typeContext: TypeContext[CT, CV])
  extends ((VersionInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], Logger[CV], ColumnInfo => SqlColumnRep[CT, CV]) => Loader[CV])
{ self =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]): DataSqlizer[CT, CV]

  def apply(versionInfo: VersionInfo, schema: ColumnIdMap[ColumnInfo], rowPreparer: RowPreparer[CV], logger: Logger[CV], repFor: ColumnInfo => SqlColumnRep[CT, CV]) = {
    val tableName = versionInfo.dataTableName

    val repSchema = schema.mapValuesStrict(repFor)

    val userPrimaryKeyInfo = schema.values.iterator.find(_.isUserPrimaryKey).map(_.systemId)

    val systemPrimaryKey = schema.iterator.find { case (_, colInfo) =>
      colInfo.logicalName == SystemColumns.id
    }.map(_._1).getOrElse { sys.error(s"No ${SystemColumns.id} column?") }

    val datasetContext = new RepBasedDatasetContext(typeContext, repSchema, userPrimaryKeyInfo, systemPrimaryKey, schema.filter { (_, i) => i.logicalName.startsWith(":") }.keySet)

    val sqlizer = produce(tableName, datasetContext)
    SqlLoader(conn, rowPreparer, sqlizer, logger, idProvider, executor)
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
