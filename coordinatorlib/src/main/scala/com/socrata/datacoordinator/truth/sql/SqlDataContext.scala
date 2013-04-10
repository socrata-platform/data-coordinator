package com.socrata.datacoordinator.truth
package sql

import javax.sql.DataSource
import java.sql.Connection
import java.io.Reader

import org.joda.time.DateTime
import com.rojoma.simplearm.SimpleArm
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.{SchemaLoader, DatasetContentsCopier, Loader, Logger}
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapReader, PostgresGlobalLog, PostgresDatasetMapWriter}
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.util.{TransferrableContextTimingReport, RowIdProvider}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo

trait SqlDataTypeContext extends DataTypeContext {
  val dataSource: DataSource

  type SqlRepType <: SqlColumnCommonRep[CT]
  def sqlRepForColumn(ci: ColumnInfo[CT]): SqlRepType
}

trait SqlDataWritingContext extends SqlDataTypeContext with DataWritingContext {
  type SqlRepType <: SqlColumnWriteRep[CT, CV]

  protected val loaderProvider: AbstractSqlLoaderProvider[CT, CV]

  protected final def loaderFactory(conn: Connection, now: DateTime, copyCtx: DatasetCopyContext[CT], idProvider: RowIdProvider, logger: Logger[CT, CV], timingReport: TransferrableContextTimingReport): Loader[CV] = {
    loaderProvider(conn, copyCtx, rowPreparer(now, copyCtx.schema), idProvider, logger, timingReport)
  }

  val databaseMutator: LowLevelDatabaseMutator[CT, CV]

  val datasetMutatorLockTimeout: Duration

  lazy val datasetMutator = DatasetMutator(databaseMutator, datasetMutatorLockTimeout)
}

trait SqlDataReadingContext extends SqlDataTypeContext with DataReadingContext {
  type SqlRepType <: SqlColumnReadRep[CT, CV]
}

trait PostgresDataContext extends SqlDataWritingContext with SqlDataReadingContext with ExecutionContext { self: DataSchemaContext =>
  type SqlRepType = SqlColumnRep[CT, CV]

  override val timingReport: TransferrableContextTimingReport

  protected def tablespace(s: String): Option[String]

  protected def copyIn(conn: Connection, sql: String, input: Reader): Long

  protected final lazy val loaderProvider = new AbstractSqlLoaderProvider(executorService, typeContext, sqlRepForColumn, isSystemColumn) with PostgresSqlLoaderProvider[CT, CV] {
    def copyIn(conn: Connection, sql: String, input: Reader) = self.copyIn(conn, sql, input)
  }

  final lazy val datasetReader = new SimpleArm[DatasetReader[CT, CV]] {
    def flatMap[B](f: (DatasetReader[CT, CV]) => B): B =
      using(dataSource.getConnection()) { conn =>
        conn.setAutoCommit(false)
        conn.setReadOnly(true)
        f(DatasetReader(new PostgresDatabaseReader(conn, new PostgresDatasetMapReader(conn, typeContext.typeNamespace, timingReport), sqlRepForColumn)))
      }
  }

  final lazy val databaseMutator: LowLevelDatabaseMutator[CT, CV] = {
    import com.rojoma.simplearm.{SimpleArm, Managed}
    import com.rojoma.simplearm.util._
    import com.socrata.datacoordinator.truth.universe._
    abstract class UniverseType extends Universe[CT, CV] with LoggerProvider with SchemaLoaderProvider with LoaderProvider with TruncatorProvider with DatasetContentsCopierProvider with DatasetMapWriterProvider with GlobalLogProvider
    new PostgresDatabaseMutator(new SimpleArm[UniverseType] {
      def flatMap[B](f: UniverseType => B): B = {
        // dataSource, sqlRepForColumn, newRowLogCodec, mapWriterFactory, globalLogFactory, loaderFactory, tablespace, timingReport
        using(dataSource.getConnection()) { conn =>
          conn.setAutoCommit(false)

          val universe = new UniverseType {
            import com.socrata.datacoordinator.truth.loader.sql._

            lazy val truncator = new SqlTruncator(conn)

            def schemaLoader(datasetInfo: DatasetInfo): SchemaLoader[CT] =
              new RepBasedPostgresSchemaLoader(conn, logger(datasetInfo), sqlRepForColumn, tablespace)

            def datasetContentsCopier(datasetInfo: DatasetInfo): DatasetContentsCopier[CT] =
              new RepBasedSqlDatasetContentsCopier(conn, logger(datasetInfo), sqlRepForColumn, timingReport)

            var loggerCache = Map.empty[String, Logger[CT, CV]]

            def logger(datasetInfo: DatasetInfo): Logger[CT, CV] =
              loggerCache.get(datasetInfo.logTableName) match {
                case Some(logger) =>
                  logger
                case None =>
                  val logger = new SqlLogger[CT, CV](conn, datasetInfo.logTableName, newRowLogCodec, timingReport)
                  loggerCache += datasetInfo.logTableName -> logger
                  logger
              }

            def loader(copyCtx: DatasetCopyContext[CT], rowIdProvider: RowIdProvider, logger: Logger[CT, CV]): Managed[Loader[CV]] =
              new SimpleArm[Loader[CV]] {
                def flatMap[B](f: Loader[CV] => B): B = {
                  f(loaderProvider(conn, copyCtx, rowPreparer(transactionStart, copyCtx.schema), rowIdProvider, logger, timingReport))
                }
              }

            def commit() {
              loggerCache.values.foreach(_.close())
              loggerCache = Map.empty
              conn.commit()
              transactionStart = DateTime.now()
            }

            val timingReport = self.timingReport
            var transactionStart: DateTime = DateTime.now()
            val globalLog: GlobalLog = new PostgresGlobalLog(conn)
            val datasetMapWriter: DatasetMapWriter[CT] = new PostgresDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, obfuscationKeyGenerator, initialRowId)
          }

          val result = f(universe)
          universe.commit()
          result
        }
      }
    })
  }
}
