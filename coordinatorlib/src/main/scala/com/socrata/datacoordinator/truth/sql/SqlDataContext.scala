package com.socrata.datacoordinator.truth
package sql

import javax.sql.DataSource
import java.sql.Connection
import java.util.concurrent.ExecutorService

import org.joda.time.DateTime
import com.socrata.id.numeric.IdProvider

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.{Loader, Logger}
import java.io.Reader

trait SqlDataContext extends DataContext {
  val dataSource: DataSource
  def repForColumn(physicalColumnBase: String, typ: CT): SqlColumnRep[CT, CV]
  def repForColumn(ci: ColumnInfo): SqlColumnRep[CT, CV] = repForColumn(ci.physicalColumnBase, typeContext.typeFromName(ci.typeName))

  val loaderProvider: AbstractSqlLoaderProvider[CT, CV]

  def loaderFactory(conn: Connection, now: DateTime, copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], idProvider: IdProvider, logger: Logger[CV]): Loader[CV] = {
    loaderProvider(conn, copy, schema, rowPreparer(now, schema), idProvider, logger)
  }
}

trait PostgresDataContext extends SqlDataContext {
  val executorService: ExecutorService

  def tablespace(s: String): Option[String]

  def copyIn(conn: Connection, sql: String, input: Reader): Long

  val loaderProvider = new AbstractSqlLoaderProvider(executorService, typeContext, repForColumn, isSystemColumn) with PostgresSqlLoaderProvider[CT, CV] {
    def copyIn(conn: Connection, sql: String, input: Reader) = PostgresDataContext.this.copyIn(conn, sql, input)
  }

  val databaseMutator: LowLevelMonadicDatabaseMutator[CV] =
    new PostgresMonadicDatabaseMutator[CT, CV](dataSource, repForColumn, newRowLogCodec, loaderFactory, tablespace)
}
