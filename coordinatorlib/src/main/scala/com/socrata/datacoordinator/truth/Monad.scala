package com.socrata.datacoordinator
package truth

import scalaz._
import scalaz.effect._
import Scalaz._
import org.joda.time.DateTime
import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.RowId

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
  type DatabaseM[+T] = Kleisli[IO, MutationContext, T]

  def runTransaction[A](action: DatabaseM[A]): IO[A]

  def now: DatabaseM[DateTime]
  def datasetMap: DatabaseM[DatasetMapWriter]
  def logger(info: DatasetInfo): DatabaseM[Logger[CV]]
  def schemaLoader(logger: Logger[CV]): DatabaseM[SchemaLoader]
  def datasetContentsCopier(logger: Logger[CV]): DatabaseM[DatasetContentsCopier]
  def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: Loader[CV] => IO[A]): DatabaseM[(Report[CV], RowId, A)]

  def globalLog: DatabaseM[GlobalLog]
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

  def loadLatestVersionOfDataset(datasetName: String): DatabaseM[Option[(CopyInfo, ColumnIdMap[ColumnInfo])]] =
    datasetMap.flatMap { map =>
      io {
        map.datasetInfo(datasetName) map { datasetInfo =>
          val latest = map.latest(datasetInfo)
          val schema = map.schema(latest)
          (latest, schema)
        }
      }
    }
}

trait MonadicDatasetMutator[CV] {
  val databaseMutator: LowLevelMonadicDatabaseMutator[CV]

  type MutationContext
  type DatasetM[+T] = StateT[databaseMutator.DatabaseM, MutationContext, T]

  def creatingDataset[A](as: String)(datasetName: String, tableBaseBase: String)(action: DatasetM[A]): IO[A]
  def withDataset[A](as: String)(datasetName: String)(action: DatasetM[A]): IO[Option[A]]
  def creatingCopy[A](as: String)(datasetName: String, copyData: Boolean)(action: DatasetM[A]): IO[Option[A]]

  def io[A](op: => A): DatasetM[A]

  def copyInfo: DatasetM[CopyInfo]
  def schema: DatasetM[ColumnIdMap[ColumnInfo]]

  def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo]
  def makeSystemPrimaryKey(ci: ColumnInfo): DatasetM[ColumnInfo]
  def makeUserPrimaryKey(ci: ColumnInfo): DatasetM[ColumnInfo]
  def dropColumn(ci: ColumnInfo): DatasetM[Unit]
  def publish: DatasetM[CopyInfo]
  def upsert(inputGenerator: ColumnIdMap[ColumnInfo] => Managed[Iterator[Either[CV, Row[CV]]]]): DatasetM[Report[CV]]
  def drop: DatasetM[Unit]
}

object MonadicDatasetMutator {
  private class Impl[CV](val databaseMutator: LowLevelMonadicDatabaseMutator[CV]) extends MonadicDatasetMutator[CV] {
    import StateT_Helper._

    case class S(currentVersion: CopyInfo, currentSchema: ColumnIdMap[ColumnInfo], schemaLoader: SchemaLoader, logger: Logger[CV])
    type MutationContext = S

    val get: DatasetM[S] = initT
    def put(s: S): DatasetM[Unit] = putT(s)
    def modify(f: S => S): DatasetM[Unit] = modifyT(f)
    def io[A](op: => A) = liftM(databaseMutator.io(op))

    val now: DatasetM[DateTime] = liftM(databaseMutator.now)
    val datasetMap: DatasetM[DatasetMapWriter] = liftM(databaseMutator.datasetMap)
    val schemaLoader: DatasetM[SchemaLoader] = getsT(_.schemaLoader)
    val logger: DatasetM[Logger[CV]] = getsT(_.logger)
    val datasetContentsCopier: DatasetM[DatasetContentsCopier] = for {
      l <- logger
      res <- liftM(databaseMutator.datasetContentsCopier(l))
    } yield res

    val copyInfo: DatasetM[CopyInfo] = getsT(_.currentVersion)
    val schema: DatasetM[ColumnIdMap[ColumnInfo]] = getsT(_.currentSchema)

    def withDataset[A](username: String)(datasetName: String)(action: DatasetM[A]): IO[Option[A]] =
      databaseMutator.runTransaction {
        for {
          initialStateOpt <- databaseMutator.loadLatestVersionOfDataset(datasetName)
          result <- initialStateOpt.map { case (initialCopy, initialSchema) =>
            for {
              logger <- databaseMutator.logger(initialCopy.datasetInfo)
              schemaLoader <- databaseMutator.schemaLoader(logger)
              initialState = S(initialCopy, initialSchema, schemaLoader, logger)
              (finalState, result) <- action(initialState)
              _ <- databaseMutator.finishDatasetTransaction(username, finalState.currentVersion, logger)
            } yield Some(result)
          }.getOrElse(None.pure[databaseMutator.DatabaseM])
        } yield result
      }

    def creatingDataset[A](username: String)(datasetName: String, tableBaseBase: String)(action: DatasetM[A]): IO[A] =
      databaseMutator.runTransaction {
        for {
          m <- databaseMutator.datasetMap
          firstVersion <- databaseMutator.io(m.create(datasetName, tableBaseBase))
          logger <- databaseMutator.logger(firstVersion.datasetInfo)
          schemaLoader <- databaseMutator.schemaLoader(logger)
          _ <- databaseMutator.io { schemaLoader.create(firstVersion) }
          initialState = S(firstVersion, ColumnIdMap.empty, schemaLoader, logger)
          (finalState, result) <- action(initialState)
          _ <- databaseMutator.finishDatasetTransaction(username, finalState.currentVersion, logger)
        } yield result
      }

    def creatingCopy[A](username: String)(datasetName: String, copyData: Boolean)(action: DatasetM[A]): IO[Option[A]] =
      withDataset(username)(datasetName) {
        makeWorkingCopy(copyData).flatMap(_ => action)
      }

    def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo] = for {
      map <- datasetMap
      s <- get
      ci <- io {
        val newColumn = map.addColumn(s.currentVersion, logicalName, typeName, physicalColumnBaseBase)
        s.schemaLoader.addColumn(newColumn)
        newColumn
      }
      _ <- put(s.copy(currentSchema = s.currentSchema + (ci.systemId -> ci)))
    } yield ci

    def dropColumn(ci: ColumnInfo): DatasetM[Unit] = for {
      map <- datasetMap
      s <- get
      _ <- io {
        map.dropColumn(ci)
        s.schemaLoader.dropColumn(ci)
      }
      _ <- put(s.copy(currentSchema = s.currentSchema - ci.systemId))
    } yield ()

    def makeSystemPrimaryKey(ci: ColumnInfo): DatasetM[ColumnInfo] = for {
      map <- datasetMap
      s <- get
      newCi <- io {
        val result = map.setSystemPrimaryKey(ci)
        val ok = s.schemaLoader.makeSystemPrimaryKey(result)
        require(ok, "Column cannot be made a system primary key")
        result
      }
      _ <- put(s.copy(currentSchema = s.currentSchema + (ci.systemId -> newCi)))
    } yield newCi

    def makeUserPrimaryKey(ci: ColumnInfo): DatasetM[ColumnInfo] = for {
      map <- datasetMap
      s <- get
      newCi <- io {
        val result = map.setUserPrimaryKey(ci)
        val ok = s.schemaLoader.makePrimaryKey(result)
        require(ok, "Column cannot be made a primary key")
        result
      }
      _ <- put(s.copy(currentSchema = s.currentSchema + (ci.systemId -> newCi)))
    } yield newCi

    def makeWorkingCopy(copyData: Boolean): DatasetM[CopyInfo] = for {
      map <- datasetMap
      dataCopier <- if(copyData) datasetContentsCopier.map(Some(_)) else None.pure[DatasetM]
      s <- get
      (newCi, newSchema) <- io {
        map.ensureUnpublishedCopy(s.currentVersion.datasetInfo) match {
          case Left(_) =>
            sys.error("Already a working copy") // TODO: Better error
          case Right(CopyPair(oldCopy, newCopy)) =>
            assert(oldCopy == s.currentVersion)

            // Great.  Now we can actually do the data loading.
            s.schemaLoader.create(newCopy)
            val schema = map.schema(newCopy)
            for(ci <- schema.values) {
              s.schemaLoader.addColumn(ci)
            }

            dataCopier.foreach(_.copy(oldCopy, newCopy, schema))

            val oldSchema = s.currentSchema
            val finalSchema = new MutableColumnIdMap[ColumnInfo]
            for(ci <- schema.values) {
              val ci2 = if(oldSchema(ci.systemId).isSystemPrimaryKey) {
                val pkified = map.setSystemPrimaryKey(ci)
                s.schemaLoader.makeSystemPrimaryKey(pkified)
                pkified
              } else {
                ci
              }

              val ci3 = if(oldSchema(ci2.systemId).isUserPrimaryKey) {
                val pkified = map.setUserPrimaryKey(ci2)
                s.schemaLoader.makePrimaryKey(pkified)
                pkified
              } else {
                ci2
              }
              finalSchema(ci3.systemId) = ci3
            }

            (newCopy, finalSchema.freeze())
        }
      }
      _ <- put(s.copy(currentVersion = newCi, currentSchema = newSchema))
    } yield newCi

    val publish: DatasetM[CopyInfo] = for {
      s <- get
      map <- datasetMap
      (ci, newSchema) <- io {
        val newCi = map.publish(s.currentVersion)
        s.logger.workingCopyPublished()
        (newCi, map.schema(newCi))
      }
      _ <- put(s.copy(currentVersion = ci, currentSchema = newSchema))
    } yield ci

    def upsert(inputGenerator: ColumnIdMap[ColumnInfo] => Managed[Iterator[Either[CV, Row[CV]]]]): DatasetM[Report[CV]] = for {
      s <- get
      (report, nextRowId, _) <- (liftM(databaseMutator.withDataLoader(s.currentVersion, s.currentSchema, s.logger) { loader =>
        IO {
          for {
            it <- inputGenerator(s.currentSchema)
            op <- it
          } {
            op match {
              case Right(row) => loader.upsert(row)
              case Left(id) => loader.delete(id)
            }
          }
        }
      }) : DatasetM[(Report[CV], RowId, Unit)])
      map <- datasetMap
      newCv <- io { map.updateNextRowId(s.currentVersion, nextRowId) }
      _ <- put(s.copy(currentVersion = newCv))
    } yield report

    val drop: DatasetM[Unit] = for {
      map <- datasetMap
      s <- get
      (newCurrentVersion, newCurrentSchema) <- io {
        map.dropCopy(s.currentVersion)
        s.schemaLoader.drop(s.currentVersion)
        val ci = map.latest(s.currentVersion.datasetInfo)
        (ci, map.schema(ci))
      }
      _ <- put(s.copy(currentVersion = newCurrentVersion, currentSchema = newCurrentSchema))
    } yield ()
  }

  def apply[CV](lowLevelMutator: LowLevelMonadicDatabaseMutator[CV]): MonadicDatasetMutator[CV] = new Impl(lowLevelMutator)
}
