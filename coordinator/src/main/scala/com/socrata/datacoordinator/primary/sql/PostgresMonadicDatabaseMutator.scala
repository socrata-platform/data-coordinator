package com.socrata.datacoordinator.primary
package sql

import java.sql.Connection
import javax.sql.DataSource

import org.joda.time.DateTime
import scalaz._
import scalaz.effect._
import Scalaz._
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, GlobalLog, DatasetMapWriter, DatasetInfo}
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.{DatasetContentsCopier, Logger, SchemaLoader, Loader, Report, RowPreparer}
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSqlSchemaLoader, RepBasedSqlDatasetContentsCopier, SqlLogger}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.{TypeContext, RowLogCodec}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId
import com.socrata.id.numeric.IdProvider
import java.util.concurrent.ExecutorService

class PostgresMonadicDatabaseMutator[CT, CV](openConnection: IO[Connection],
                                             repForColumn: ColumnInfo => SqlColumnRep[CT, CV],
                                             rowCodecFactory: () => RowLogCodec[CV],
                                             typeContext: TypeContext[CT, CV],
                                             rowPreparer: (DateTime, ColumnIdMap[ColumnInfo]) => RowPreparer[CV],
                                             executor: ExecutorService,
                                             rowFlushSize: Int = 128000,
                                             batchFlushSize: Int = 2000000)
  extends LowLevelMonadicDatabaseMutator[CV]
{
  import StateT_Helper._

  type LoaderProvider = (CopyInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], IdProvider, Logger[CV], ColumnInfo => SqlColumnRep[CT, CV]) => Loader[CV]

  case class S(conn: Connection, now: DateTime, datasetMap: DatasetMapWriter, globalLog: GlobalLog, loaderProvider: LoaderProvider)
  type MutationContext = S

  def closeConnection(conn: Connection): IO[Unit] = IO(conn.rollback()).ensuring(IO(conn.close()))
  def withConnection[A](f: Connection => IO[A]): IO[A] =
    openConnection.bracket(closeConnection)(f)

  def createInitialState(conn: Connection): IO[S] = IO {
    val datasetMap = new PostgresDatasetMapWriter(conn)
    val globalLog = new PostgresGlobalLog(conn)
    val now = for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("SELECT current_timestamp"))
    } yield {
      rs.next()
      new DateTime(rs.getTimestamp(1).getTime)
    }
    val lp = new com.socrata.datacoordinator.common.sql.AbstractSqlLoaderProvider(conn, executor, typeContext) with com.socrata.datacoordinator.common.sql.PostgresSqlLoaderProvider[CT, CV]
    S(conn, now, datasetMap, globalLog, lp)
  }

  def runTransaction[A](action: DatabaseM[A]): IO[A] =
    withConnection { conn =>
      for {
        _ <- IO(conn.setAutoCommit(false))
        initialState <- createInitialState(conn)
        (finalState, result) <- action.run(initialState)
        _ <- IO(conn.commit())
      } yield result
    }

  val get: DatabaseM[S] = initT
  def set(s: S): DatabaseM[Unit] = putT(s)
  def io[A](f: => A) = liftM(IO(f))

  val rawNow: DatabaseM[DateTime] = getsT(_.now)
  val rawConn: DatabaseM[Connection] = getsT(_.conn)
  def loaderProvider: DatabaseM[LoaderProvider] = getsT(_.loaderProvider)

  val datasetMap: DatabaseM[DatasetMapWriter] = getsT(_.datasetMap)

  def schemaLoader(logger: Logger[CV]): DatabaseM[SchemaLoader] =
    rawConn.map(new RepBasedSqlSchemaLoader(_, logger, repForColumn))

  def logger(datasetInfo: DatasetInfo): DatabaseM[Logger[CV]] =
    rawConn.map(new SqlLogger(_, datasetInfo.logTableName, rowCodecFactory, rowFlushSize, batchFlushSize))

  def datasetContentsCopier(logger: Logger[CV]): DatabaseM[DatasetContentsCopier] =
    rawConn.map(new RepBasedSqlDatasetContentsCopier(_, logger, repForColumn))

  def dataLoader(logger: Logger[CV]): DatabaseM[Loader[CV]] = ???

  def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: Loader[CV] => IO[A]): DatabaseM[(Report[CV], RowId, A)] = for {
    s <- get
    res <- io {
      val rowIdProvider = new com.socrata.datacoordinator.util.RowIdProvider(table.datasetInfo.nextRowId)
      using(s.loaderProvider(table, schema, rowPreparer(s.now, schema), rowIdProvider, logger, repForColumn)) { loader =>
        val result = f(loader).unsafePerformIO()
        val report = loader.report
        (report, rowIdProvider.finish(), result)
      }
    }
  } yield res

  val globalLog: DatabaseM[GlobalLog] = getsT(_.globalLog)

  val now: DatabaseM[DateTime] = getsT(_.now)
}
