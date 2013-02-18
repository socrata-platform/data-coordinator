package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._
import org.joda.time.DateTime
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader.{Logger, SchemaLoader}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import scala.Some
import com.socrata.datacoordinator.truth.metadata.CopyInfo

object StateT_Helper {
  import scala.language.higherKinds
  // Some combinators and lifted operations for StateT
  def lift[M[+_]: Monad, S, A](f: State[S, A]) = StateT[M, S, A](f(_).pure[M])
  def liftM[M[+_]: Functor, S, A](f: M[A]) = StateT[M, S, A](s => f.map((s, _)))
  def initT[M[+_]: Monad, S] = lift[M, S, S](init[S])
  def modifyT[M[+_]: Monad, S](f: S => S) = lift[M, S, Unit](modify[S](f))
  def putT[M[+_]: Monad, S](s: S) = lift[M, S, Unit](put(s))
  def getsT[M[+_]: Monad, S, A](f: S => A) = lift[M, S, A](gets(f))
}

trait LowLevelMonadicDatabaseMutator[CV] {
  type MutationContext
  type DatabaseM[+T] = StateT[IO, MutationContext, T]

  def runTransaction[A](action: DatabaseM[A]): IO[A]

  val now: DatabaseM[DateTime]
  val datasetMap: DatabaseM[DatasetMapWriter]
  def schemaLoader(logger: Logger[CV]): DatabaseM[SchemaLoader]
  def logger(info: DatasetInfo): DatabaseM[Logger[CV]]
  val globalLog: DatabaseM[GlobalLog]
  def io[A](op: => A): DatabaseM[A]

  def finishDatasetTransaction[A](username: String, copyInfo: CopyInfo, logger: Logger[CV]): DatabaseM[Unit] = for {
    globLog <- globalLog
    map <- datasetMap
    now <- now
    _ <- io {
      logger.endTransaction() foreach { ver =>
        map.updateDataVersion(copyInfo, ver)
        globLog.log(copyInfo.datasetInfo, ver, now, username)
      }
    }
  } yield ()

  def loadLatestVersionOfDataset(datasetName: String): DatabaseM[Option[(CopyInfo, ColumnIdMap[ColumnInfo])]] = for {
    map <- datasetMap
    result <- io {
      map.datasetInfo(datasetName) map { datasetInfo =>
        val latest = map.latest(datasetInfo)
        val schema = map.schema(latest)
        (latest, schema)
      }
    }
  } yield result
}

trait MonadicDatasetMutator[CV] {
  val databaseMutator: LowLevelMonadicDatabaseMutator[CV]

  type MutationContext
  type DatasetM[+T] = StateT[databaseMutator.DatabaseM, MutationContext, T]

  def withDataset[A](datasetName: String, username: String)(action: DatasetM[A]): IO[Option[A]]
  def creatingDataset[A](datasetName: String, tableBaseBase: String, username: String)(action: DatasetM[A]): IO[A]

  def io[A](op: => A): DatasetM[A]

  val copyInfo: DatasetM[CopyInfo]
  val schema: DatasetM[ColumnIdMap[ColumnInfo]]

  def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo]
  def dropColumn(ci: ColumnInfo): DatasetM[Unit]
}

class MonadicDatabaseMutatorImpl[CV](val databaseMutator: LowLevelMonadicDatabaseMutator[CV]) extends MonadicDatasetMutator[CV] {
  import StateT_Helper._

  case class S(currentVersion: CopyInfo, currentSchema: ColumnIdMap[ColumnInfo], schemaLoader: SchemaLoader)
  type MutationContext = S

  val get: DatasetM[S] = initT
  def set(s: S): DatasetM[Unit] = putT(s)
  def io[A](op: => A) = liftM(databaseMutator.io(op))

  val now: DatasetM[DateTime] = liftM(databaseMutator.now)
  val datasetMap: DatasetM[DatasetMapWriter] = liftM(databaseMutator.datasetMap)
  val schemaLoader: DatasetM[SchemaLoader] = getsT(_.schemaLoader)

  val copyInfo: DatasetM[CopyInfo] = getsT(_.currentVersion)
  val schema: DatasetM[ColumnIdMap[ColumnInfo]] = getsT(_.currentSchema)

  def withDataset[A](datasetName: String, username: String)(action: DatasetM[A]): IO[Option[A]] =
    databaseMutator.runTransaction {
      for {
        initialStateOpt <- databaseMutator.loadLatestVersionOfDataset(datasetName)
        result <- initialStateOpt.map { case (initialCopy, initialSchema) =>
          for {
            logger <- databaseMutator.logger(initialCopy.datasetInfo)
            schemaLoader <- databaseMutator.schemaLoader(logger)
            initialState = S(initialCopy, initialSchema, schemaLoader)
            (finalState, result) <- action(initialState)
            _ <- databaseMutator.finishDatasetTransaction(username, finalState.currentVersion, logger)
          } yield Some(result)
        }.getOrElse(None.pure[databaseMutator.DatabaseM])
      } yield result
    }

  def creatingDataset[A](datasetName: String, tableBaseBase: String, username: String)(action: DatasetM[A]): IO[A] =
    databaseMutator.runTransaction {
      for {
        m <- databaseMutator.datasetMap
        firstVersion <- databaseMutator.io(m.create(datasetName, tableBaseBase))
        logger <- databaseMutator.logger(firstVersion.datasetInfo)
        schemaLoader <- databaseMutator.schemaLoader(logger)
        _ <- databaseMutator.io { schemaLoader.create(firstVersion) }
        initialState = S(firstVersion, ColumnIdMap.empty, schemaLoader)
        (finalState, result) <- action(initialState)
        _ <- databaseMutator.finishDatasetTransaction(username, finalState.currentVersion, logger)
      } yield result
    }

  def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo] = for {
    map <- datasetMap
    sl <- schemaLoader
    s <- get
    ci <- io {
      val newColumn = map.addColumn(s.currentVersion, logicalName, typeName, physicalColumnBaseBase)
      sl.addColumn(newColumn)
      newColumn
    }
    _ <- set(s.copy(currentSchema = s.currentSchema + (ci.systemId -> ci)))
  } yield ci

  def dropColumn(ci: ColumnInfo): DatasetM[Unit] = for {
    map <- datasetMap
    sl <- schemaLoader
    s <- get
    _ <- io {
      map.dropColumn(ci)
      sl.dropColumn(ci)
    }
    _ <- set(s.copy(currentSchema = s.currentSchema - ci.systemId))
  } yield ()
}

object Test extends App {
  import org.postgresql.ds._
  import com.socrata.datacoordinator.truth.sql.SqlColumnRep
  import com.socrata.datacoordinator.common.soql._
  import com.socrata.datacoordinator.common.StandardDatasetMapLimits
  import com.socrata.soql.types.SoQLType

  val ds = new PGSimpleDataSource
  ds.setServerName("localhost")
  ds.setPortNumber(5432)
  ds.setUser("blist")
  ds.setPassword("blist")
  ds.setDatabaseName("robertm")

  val typeContext = SoQLTypeContext
  val soqlRepFactory = SoQLRep.repFactories.keys.foldLeft(Map.empty[SoQLType, String => SqlColumnRep[SoQLType, Any]]) { (acc, typ) =>
    acc + (typ -> SoQLRep.repFactories(typ))
  }
  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    soqlRepFactory(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  val ll = new sql.PostgresMonadicDatabaseMutator(ds, genericRepFor, () => SoQLRowLogCodec)
  val highlevel: MonadicDatasetMutator[Any] = new MonadicDatabaseMutatorImpl(ll)

  com.rojoma.simplearm.util.using(ds.getConnection) { conn =>
    com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
  }

  import highlevel._
  creatingDataset("mine", "m", "robertm") {
    for {
      _ <- addColumn("col1", "number", "first")
      _ <- addColumn("col2", "text", "second")
    } yield "done"
  }.unsafePerformIO()
}
