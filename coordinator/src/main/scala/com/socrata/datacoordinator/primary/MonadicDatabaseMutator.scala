package com.socrata.datacoordinator
package primary

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
  type DatabaseM[+T] = StateT[IO, MutationContext, T]

  def runTransaction[A](action: DatabaseM[A]): IO[A]

  val now: DatabaseM[DateTime]
  val datasetMap: DatabaseM[DatasetMapWriter]
  def logger(info: DatasetInfo): DatabaseM[Logger[CV]]
  def schemaLoader(logger: Logger[CV]): DatabaseM[SchemaLoader]
  def datasetContentsCopier(logger: Logger[CV]): DatabaseM[DatasetContentsCopier]
  def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: Loader[CV] => IO[A]): DatabaseM[(Report[CV], RowId, A)]

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
  def makeSystemPrimaryKey(ci: ColumnInfo): DatasetM[ColumnInfo]
  def dropColumn(ci: ColumnInfo): DatasetM[Unit]
  def upsert(inputGenerator: ColumnIdMap[ColumnInfo] => Managed[Iterator[Either[CV, Row[CV]]]]): DatasetM[Report[CV]]
}

class MonadicDatabaseMutatorImpl[CV](val databaseMutator: LowLevelMonadicDatabaseMutator[CV]) extends MonadicDatasetMutator[CV] {
  import StateT_Helper._

  case class S(currentVersion: CopyInfo, currentSchema: ColumnIdMap[ColumnInfo], schemaLoader: SchemaLoader, logger: Logger[CV])
  type MutationContext = S

  val get: DatasetM[S] = initT
  def set(s: S): DatasetM[Unit] = putT(s)
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

  def withDataset[A](datasetName: String, username: String)(action: DatasetM[A]): IO[Option[A]] =
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

  def creatingDataset[A](datasetName: String, tableBaseBase: String, username: String)(action: DatasetM[A]): IO[A] =
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

  def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo] = for {
    map <- datasetMap
    s <- get
    ci <- io {
      val newColumn = map.addColumn(s.currentVersion, logicalName, typeName, physicalColumnBaseBase)
      s.schemaLoader.addColumn(newColumn)
      newColumn
    }
    _ <- set(s.copy(currentSchema = s.currentSchema + (ci.systemId -> ci)))
  } yield ci

  def dropColumn(ci: ColumnInfo): DatasetM[Unit] = for {
    map <- datasetMap
    s <- get
    _ <- io {
      map.dropColumn(ci)
      s.schemaLoader.dropColumn(ci)
    }
    _ <- set(s.copy(currentSchema = s.currentSchema - ci.systemId))
  } yield ()

  def makeSystemPrimaryKey(ci: ColumnInfo): DatasetM[ColumnInfo] = for {
    map <- datasetMap
    s <- get
    newCi <- io {
      val result = map.setSystemPrimaryKey(ci)
      val ok = s.schemaLoader.makeSystemPrimaryKey(ci)
      require(ok, "Column cannot be made a system primary key")
      result
    }
    _ <- set(s.copy(currentSchema = s.currentSchema + (ci.systemId -> newCi)))
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
    _ <- set(s.copy(currentSchema = s.currentSchema + (ci.systemId -> newCi)))
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
    _ <- set(s.copy(currentVersion = newCi, currentSchema = newSchema))
  } yield newCi

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
    _ <- set(s.copy(currentVersion = newCv))
  } yield report
}

object Test extends App {
  import org.postgresql.ds._
  import com.socrata.datacoordinator.truth.sql.SqlColumnRep
  import com.socrata.datacoordinator.common.soql._
  import com.socrata.datacoordinator.common.StandardDatasetMapLimits
  import com.socrata.soql.types.SoQLType
  import com.socrata.datacoordinator.id.RowId
  import com.socrata.datacoordinator.util.CloseableIterator
  import com.rojoma.simplearm.util._

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

  def rowPreparer(now: DateTime, schema: ColumnIdMap[ColumnInfo]): RowPreparer[Any] =
    new RowPreparer[Any] {
      def findCol(name: String) =
        schema.values.iterator.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId
      val idColumn = findCol(SystemColumns.id)
      val createdAtColumn = findCol(SystemColumns.createdAt)
      val updatedAtColumn = findCol(SystemColumns.updatedAt)

      def prepareForInsert(row: Row[Any], sid: RowId): Row[Any] = {
        val tmp = new MutableRow[Any](row)
        tmp(idColumn) = sid
        tmp(createdAtColumn) = now
        tmp(updatedAtColumn) = now
        tmp.freeze()
      }

      def prepareForUpdate(row: Row[Any]): Row[Any] = {
        val tmp = new MutableRow[Any](row)
        tmp(updatedAtColumn) = now
        tmp.freeze()
      }
    }

  val executor = java.util.concurrent.Executors.newCachedThreadPool()
  val openConnection = IO(ds.getConnection())
  val ll = new sql.PostgresMonadicDatabaseMutator(openConnection, genericRepFor, () => SoQLRowLogCodec,typeContext,rowPreparer,executor)
  val highlevel: MonadicDatasetMutator[Any] = new MonadicDatabaseMutatorImpl(ll)

  com.rojoma.simplearm.util.using(openConnection.unsafePerformIO()) { conn =>
    com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
  }

  import highlevel._
  val report = creatingDataset(System.currentTimeMillis().toString, "m", "robertm") {
    for {
      id <- addColumn(":id", "row_identifier", "id")
      _ <- makeSystemPrimaryKey(id)
      created_at <- addColumn(":created_at", "fixed_timestamp", "created")
      updated_at <- addColumn(":updated_at", "fixed_timestamp", "updated")
      col1 <- addColumn("col1", "number", "first")
      col2 <- addColumn("col2", "text", "second")
      report <- upsert { _ =>
        managed {
          CloseableIterator.simple(Iterator(
            Right(Row(col1.systemId -> BigDecimal(5), col2.systemId -> "hello")),
            Right(Row(col1.systemId -> BigDecimal(6), col2.systemId -> "hello"))
          ))
        }
      }
    } yield report
  }.unsafePerformIO()
  println(report)

  executor.shutdown()
}
