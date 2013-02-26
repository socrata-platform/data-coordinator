package com.socrata.datacoordinator.truth
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
import com.socrata.datacoordinator.truth.{TypeContext, RowLogCodec}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId
import com.socrata.id.numeric.IdProvider
import java.util.concurrent.ExecutorService

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
class PostgresMonadicDatabaseMutator[CT, CV](dataSource: DataSource,
                                             repForColumn: ColumnInfo => SqlColumnRep[CT, CV],
                                             rowCodecFactory: () => RowLogCodec[CV],
                                             mapWriterFactory: Connection => DatasetMapWriter,
                                             globalLogFactory: Connection => GlobalLog,
                                             loaderFactory: (Connection, DateTime, CopyInfo, ColumnIdMap[ColumnInfo], IdProvider, Logger[CV]) => Loader[CV],
                                             tablespace: String => Option[String],
                                             rowFlushSize: Int = 128000,
                                             batchFlushSize: Int = 2000000)
  extends DatabaseIO(dataSource) with LowLevelMonadicDatabaseMutator[CV]
{
  import Kleisli.ask

  type LoaderProvider = (CopyInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], IdProvider, Logger[CV], ColumnInfo => SqlColumnRep[CT, CV]) => Loader[CV]

  case class S(conn: Connection, now: DateTime, datasetMap: DatasetMapWriter, globalLog: GlobalLog)
  type MutationContext = S

  def createInitialState(conn: Connection): IO[S] = IO {
    val datasetMap = mapWriterFactory(conn)
    val globalLog = globalLogFactory(conn)
    val now = for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("SELECT current_timestamp"))
    } yield {
      rs.next()
      new DateTime(rs.getTimestamp(1).getTime)
    }
    S(conn, now, datasetMap, globalLog)
  }

  def runTransaction[A](action: DatabaseM[A]): IO[A] =
    withConnection { conn =>
      for {
        _ <- IO(conn.setAutoCommit(false))
        initialState <- createInitialState(conn)
        result <- action.run(initialState)
        _ <- IO(conn.commit())
      } yield result
    }

  val get: DatabaseM[S] = ask
  import scala.language.higherKinds
  def io[A](f: => A) = IO(f).liftKleisli[S]

  val rawNow: DatabaseM[DateTime] = get.map(_.now)
  val rawConn: DatabaseM[Connection] = get.map(_.conn)

  val datasetMap: DatabaseM[DatasetMapWriter] = get.map(_.datasetMap)

  def schemaLoader(logger: Logger[CV]): DatabaseM[SchemaLoader] =
    rawConn.map(new RepBasedSqlSchemaLoader(_, logger, repForColumn, tablespace))

  def logger(datasetInfo: DatasetInfo): DatabaseM[Logger[CV]] =
    rawConn.map(new SqlLogger(_, datasetInfo.logTableName, rowCodecFactory, rowFlushSize, batchFlushSize))

  def datasetContentsCopier(logger: Logger[CV]): DatabaseM[DatasetContentsCopier] =
    rawConn.map(new RepBasedSqlDatasetContentsCopier(_, logger, repForColumn))

  def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: Loader[CV] => IO[A]): DatabaseM[(Report[CV], RowId, A)] = for {
    s <- get
    res <- io {
      val rowIdProvider = new com.socrata.datacoordinator.util.RowIdProvider(table.datasetInfo.nextRowId)
      using(loaderFactory(s.conn, s.now, table, schema, rowIdProvider, logger)) { loader =>
        val result = f(loader).unsafePerformIO()
        val report = loader.report
        (report, rowIdProvider.finish(), result)
      }
    }
  } yield res

  val globalLog: DatabaseM[GlobalLog] = get.map(_.globalLog)

  val now: DatabaseM[DateTime] = get.map(_.now)
}
