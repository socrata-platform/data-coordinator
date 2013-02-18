package com.socrata.datacoordinator.primary
package sql

import java.sql.Connection
import javax.sql.DataSource

import org.joda.time.DateTime
import scalaz._
import scalaz.effect._
import Scalaz._
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, GlobalLog, DatasetMapWriter, DatasetInfo}
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.{Logger, SchemaLoader}
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSqlSchemaLoader, SqlLogger}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.RowLogCodec

class PostgresMonadicDatabaseMutator[CT, CV](dataSource: DataSource,
                                             repForColumn: ColumnInfo => SqlColumnRep[CT, CV],
                                             rowCodecFactory: () => RowLogCodec[CV],
                                             rowFlushSize: Int = 128000,
                                             batchFlushSize: Int = 2000000)
  extends LowLevelMonadicDatabaseMutator[CV]
{
  import StateT_Helper._

  case class S(conn: Connection, now: DateTime, datasetMap: DatasetMapWriter, globalLog: GlobalLog)
  type MutationContext = S

  def openConnection: IO[Connection] = IO(dataSource.getConnection())
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
    S(conn, now, datasetMap, globalLog)
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

  val datasetMap: DatabaseM[DatasetMapWriter] = getsT(_.datasetMap)

  def schemaLoader(logger: Logger[CV]): DatabaseM[SchemaLoader] =
    rawConn.map(new RepBasedSqlSchemaLoader(_, logger, repForColumn))

  def logger(datasetInfo: DatasetInfo): DatabaseM[Logger[CV]] =
    rawConn.map(new SqlLogger(_, datasetInfo.logTableName, rowCodecFactory, rowFlushSize, batchFlushSize))

  val globalLog: DatabaseM[GlobalLog] = getsT(_.globalLog)

  val now: DatabaseM[DateTime] = getsT(_.now)
}
