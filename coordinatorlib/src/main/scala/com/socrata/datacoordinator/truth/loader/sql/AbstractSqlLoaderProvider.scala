package com.socrata.datacoordinator.truth.loader.sql

import java.util.concurrent.ExecutorService
import java.sql.Connection

import com.socrata.datacoordinator.truth.TypeContext
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.{Loader, Logger, RowPreparer}
import java.io.Reader
import com.socrata.datacoordinator.util.{TransferrableContextTimingReport, RowIdProvider, TimingReport}

abstract class AbstractSqlLoaderProvider[CT, CV](val executor: ExecutorService, typeContext: TypeContext[CT, CV], repFor: ColumnInfo => SqlColumnRep[CT, CV], isSystemColumn: ColumnInfo => Boolean, timingReport: TransferrableContextTimingReport)
  extends ((Connection, CopyInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], RowIdProvider, Logger[CV]) => Loader[CV])
{
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]): DataSqlizer[CT, CV]

  def apply(conn: Connection, versionInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo], rowPreparer: RowPreparer[CV], idProvider: RowIdProvider, logger: Logger[CV]) = {
    val tableName = versionInfo.dataTableName

    val repSchema = schema.mapValuesStrict(repFor)

    val userPrimaryKeyInfo = schema.values.iterator.find(_.isUserPrimaryKey).map(_.systemId)

    val systemPrimaryKey = schema.values.find(_.isSystemPrimaryKey).map(_.systemId).getOrElse {
      sys.error(s"No system primary key column?")
    }

    val datasetContext = RepBasedSqlDatasetContext(typeContext, repSchema, userPrimaryKeyInfo, systemPrimaryKey, schema.filter { case (_, ci) => isSystemColumn(ci) }.keySet)

    val sqlizer = produce(tableName, datasetContext)
    SqlLoader(conn, rowPreparer, sqlizer, logger, idProvider, executor, timingReport)
  }
}

trait StandardSqlLoaderProvider[CT, CV] { this: AbstractSqlLoaderProvider[CT, CV] =>
  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new StandardRepBasedDataSqlizer(tableName, datasetContext)
}

trait PostgresSqlLoaderProvider[CT, CV] { this: AbstractSqlLoaderProvider[CT, CV] =>
  def copyIn(conn: Connection, sql: String, reader: Reader): Long

  def produce(tableName: String, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new PostgresRepBasedDataSqlizer(tableName, datasetContext, executor, copyIn)
}
