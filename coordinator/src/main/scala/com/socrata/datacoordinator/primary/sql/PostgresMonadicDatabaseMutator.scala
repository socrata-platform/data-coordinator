package com.socrata.datacoordinator.primary
package sql

import scala.language.higherKinds

import java.sql.Connection
import javax.sql.DataSource

import org.joda.time.DateTime
import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, DatasetMapWriter}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.truth.loader.SchemaLoader

case class S(now: DateTime, datasetMap: DatasetMapWriter, schemaLoader: SchemaLoader, currentVersion: CopyInfo, currentSchema: ColumnIdMap[ColumnInfo])

class PostgresMonadicDatabaseMutator(dataSource: DataSource) extends MonadicDatabaseMutator {
  type MutationContext = S

  def openConnection: IO[Connection] = IO(dataSource.getConnection())
  def closeConnection(conn: Connection): IO[Unit] = IO(conn.close())
  def withConnection[A](f: Connection => IO[A]): IO[A] =
    openConnection.bracket(closeConnection)(f)

  def createInitialState(conn: Connection, datasetName: String): IO[Option[S]] = IO {
    val datasetMap = new PostgresDatasetMapWriter(conn)
    datasetMap.datasetInfo(datasetName) map { info =>
      val initialVersion = datasetMap.latest(info)
      val initialSchema = datasetMap.schema(initialVersion)
      val schemaLoader = ???
      S(DateTime.now(), datasetMap, schemaLoader, initialVersion, initialSchema)
    }
  }

  def runTransaction[A](datasetName: String)(action: DatasetM[A]): IO[Option[A]] =
    withConnection { conn =>
      for {
        _ <- IO(conn.setAutoCommit(false))
        initialStateOpt <- createInitialState(conn, datasetName)
        result <- initialStateOpt.map(action.eval(_).map(Some(_))).getOrElse(IO(None))
        _ <- IO(conn.commit())
      } yield result
    }

  val get: DatasetM[S] = initT
  def set(s: S): DatasetM[Unit] = putT(s)
  val now: DatasetM[DateTime] = getsT(_.now)
  val copyInfo: DatasetM[CopyInfo] = getsT(_.currentVersion)
  val schema: DatasetM[ColumnIdMap[ColumnInfo]] = getsT(_.currentSchema)
  def io[A](f: => A) = liftM(IO(f))

  def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo] = for {
    s <- get
    ci <- io {
      val newColumn = s.datasetMap.addColumn(s.currentVersion, logicalName, typeName, physicalColumnBaseBase)
      s.schemaLoader.addColumn(newColumn)
      newColumn
    }
    _ <- set(s.copy(currentSchema = s.currentSchema + (ci.systemId -> ci)))
  } yield ci

  def dropColumn(ci: ColumnInfo): DatasetM[Unit] = for {
    s <- get
    _ <- io {
      s.datasetMap.dropColumn(ci)
      s.schemaLoader.dropColumn(ci)
    }
    _ <- set(s.copy(currentSchema = s.currentSchema - ci.systemId))
  } yield ()

  // Some combinators and lifted operations for StateT
  def lift[M[+_]: Monad, S, A](f: State[S, A]) = StateT[M, S, A](f(_).pure[M])
  def liftM[M[+_]: Functor, S, A](f: M[A]) = StateT[M, S, A](s => f.map((s, _)))
  def initT[M[+_]: Monad, S] = lift[M, S, S](init[S])
  def modifyT[M[+_]: Monad, S](f: S => S) = lift[M, S, Unit](modify[S](f))
  def putT[M[+_]: Monad, S](s: S) = lift[M, S, Unit](put(s))
  def getsT[M[+_]: Monad, S, A](f: S => A) = lift[M, S, A](gets(f))
}
