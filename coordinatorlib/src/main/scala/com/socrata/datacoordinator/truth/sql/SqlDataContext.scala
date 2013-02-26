package com.socrata.datacoordinator.truth
package sql

import javax.sql.DataSource
import java.sql.Connection

import org.joda.time.DateTime
import com.socrata.id.numeric.IdProvider

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.{Loader, Logger}
import java.io.Reader
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapReader, PostgresGlobalLog, PostgresDatasetMapWriter}

trait SqlDataTypeContext extends DataTypeContext {
  val dataSource: DataSource

  type SqlRepType <: SqlColumnCommonRep[CT]
  def sqlRepForColumn(physicalColumnBase: String, typ: CT): SqlRepType
  final def sqlRepForColumn(ci: ColumnInfo): SqlRepType = sqlRepForColumn(ci.physicalColumnBase, typeContext.typeFromName(ci.typeName))
}

trait SqlDataWritingContext extends SqlDataTypeContext with DataWritingContext {
  type SqlRepType <: SqlColumnWriteRep[CT, CV]

  protected val loaderProvider: AbstractSqlLoaderProvider[CT, CV]

  protected final def loaderFactory(conn: Connection, now: DateTime, copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], idProvider: IdProvider, logger: Logger[CV]): Loader[CV] = {
    loaderProvider(conn, copy, schema, rowPreparer(now, schema), idProvider, logger)
  }

  val databaseMutator: LowLevelMonadicDatabaseMutator[CV]

  final lazy val datasetMutator = MonadicDatasetMutator(databaseMutator)
}

trait SqlDataReadingContext extends SqlDataTypeContext with DataReadingContext {
  type SqlRepType <: SqlColumnReadRep[CT, CV]

  val databaseReader: LowLevelMonadicDatabaseReader[CV]
  final lazy val datasetReader = MonadicDatasetReader(databaseReader)
}

trait PostgresDataContext extends SqlDataWritingContext with SqlDataReadingContext { this: DataSchemaContext with ExecutionContext =>
  type SqlRepType = SqlColumnRep[CT, CV]

  protected def tablespace(s: String): Option[String]

  protected def copyIn(conn: Connection, sql: String, input: Reader): Long

  protected final lazy val loaderProvider = new AbstractSqlLoaderProvider(executorService, typeContext, sqlRepForColumn, isSystemColumn) with PostgresSqlLoaderProvider[CT, CV] {
    def copyIn(conn: Connection, sql: String, input: Reader) = PostgresDataContext.this.copyIn(conn, sql, input)
  }

  private def mapReaderFactory(conn: Connection) = new PostgresDatasetMapReader(conn)
  private def mapWriterFactory(conn: Connection) = new PostgresDatasetMapWriter(conn)
  private def globalLogFactory(conn: Connection) = new PostgresGlobalLog(conn)

  final lazy val databaseReader: LowLevelMonadicDatabaseReader[CV] =
    new PostgresMonadicDatabaseReader[CT, CV](dataSource, mapReaderFactory, sqlRepForColumn)

  final lazy val databaseMutator: LowLevelMonadicDatabaseMutator[CV] =
    new PostgresMonadicDatabaseMutator(dataSource, sqlRepForColumn, newRowLogCodec, mapWriterFactory, globalLogFactory, loaderFactory, tablespace)
}
