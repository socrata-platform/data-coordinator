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
import scala.concurrent.duration.Duration

trait SqlDataTypeContext extends DataTypeContext {
  val dataSource: DataSource

  type SqlRepType <: SqlColumnCommonRep[CT]
  def sqlRepForColumn(physicalColumnBase: String, typ: CT): SqlRepType
  final def sqlRepForColumn(ci: ColumnInfo): SqlRepType = sqlRepForColumn(ci.physicalColumnBase, typeContext.typeFromName(ci.typeName))
}

trait DatasetLockContext {
  val datasetLock: DatasetLock
  val datasetLockTimeout: Duration
}

trait SqlDataWritingContext extends SqlDataTypeContext with DataWritingContext { this: DatasetLockContext =>
  type SqlRepType <: SqlColumnWriteRep[CT, CV]

  protected val loaderProvider: AbstractSqlLoaderProvider[CT, CV]

  protected final def loaderFactory(conn: Connection, now: DateTime, copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], idProvider: IdProvider, logger: Logger[CV]): Loader[CV] = {
    loaderProvider(conn, copy, schema, rowPreparer(now, schema), idProvider, logger)
  }

  val databaseMutator: LowLevelDatabaseMutator[CV]

  final lazy val datasetMutator = MonadicDatasetMutator(databaseMutator, datasetLock, datasetLockTimeout)
}

trait SqlDataReadingContext extends SqlDataTypeContext with DataReadingContext {
  type SqlRepType <: SqlColumnReadRep[CT, CV]

  val databaseReader: LowLevelDatabaseReader[CV]
  final lazy val datasetReader = DatasetReader(databaseReader)
}

trait PostgresDataContext extends SqlDataWritingContext with SqlDataReadingContext { this: DataSchemaContext with ExecutionContext with DatasetLockContext =>
  type SqlRepType = SqlColumnRep[CT, CV]

  protected def tablespace(s: String): Option[String]

  protected def copyIn(conn: Connection, sql: String, input: Reader): Long

  protected final lazy val loaderProvider = new AbstractSqlLoaderProvider(executorService, typeContext, sqlRepForColumn, isSystemColumn, timingReport) with PostgresSqlLoaderProvider[CT, CV] {
    def copyIn(conn: Connection, sql: String, input: Reader) = PostgresDataContext.this.copyIn(conn, sql, input)
  }

  private def mapReaderFactory(conn: Connection) = new PostgresDatasetMapReader(conn)
  private def mapWriterFactory(conn: Connection) = new PostgresDatasetMapWriter(conn)
  private def globalLogFactory(conn: Connection) = new PostgresGlobalLog(conn)

  final lazy val databaseReader: LowLevelDatabaseReader[CV] =
    new PostgresDatabaseReader[CT, CV](dataSource, mapReaderFactory, sqlRepForColumn)

  final lazy val databaseMutator: LowLevelDatabaseMutator[CV] =
    new PostgresDatabaseMutator(dataSource, sqlRepForColumn, newRowLogCodec, mapWriterFactory, globalLogFactory, loaderFactory, tablespace, timingReport)
}
