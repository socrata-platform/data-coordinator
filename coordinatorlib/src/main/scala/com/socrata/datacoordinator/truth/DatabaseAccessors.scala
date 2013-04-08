package com.socrata.datacoordinator
package truth

import org.joda.time.DateTime
import com.rojoma.simplearm.{SimpleArm, Managed}

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.{DatasetId, RowId}
import scala.concurrent.duration.Duration
import com.socrata.soql.environment.{TypeName, ColumnName}

trait LowLevelDatabaseReader[CV] {
  trait ReadContext {
    def datasetMap: DatasetMapReader

    def loadDataset(datasetName: String, latest: CopySelector): Option[(CopyInfo, ColumnIdMap[ColumnInfo])]

    def withRows[A](ci: CopyInfo, sidCol: ColumnInfo, schema: ColumnIdMap[ColumnInfo], f: Iterator[ColumnIdMap[CV]] => A, limit: Option[Long], offset: Option[Long]): A
  }

  def openDatabase: Managed[ReadContext]
}

trait LowLevelDatabaseMutator[CV] {
  trait MutationContext {
    def now: DateTime
    def datasetMap: DatasetMapWriter
    def logger(info: DatasetInfo): Logger[CV]
    def schemaLoader(info: DatasetInfo): SchemaLoader
    def datasetContentsCopier(info: DatasetInfo): DatasetContentsCopier
    def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: Loader[CV] => A): (Report[CV], RowId, A)
    def truncate(table: CopyInfo, logger: Logger[CV])

    def globalLog: GlobalLog

    def finishDatasetTransaction(username: String, copyInfo: CopyInfo)

    def loadLatestVersionOfDataset(datasetId: DatasetId, lockTimeout: Duration): Option[(CopyInfo, ColumnIdMap[ColumnInfo])]
  }

  def openDatabase: Managed[MutationContext]
}

sealed abstract class CopySelector
case object LatestCopy extends CopySelector
case object PublishedCopy extends CopySelector
case object WorkingCopy extends CopySelector
case class Snapshot(nth: Int) extends CopySelector

trait DatasetReader[CV] {
  val databaseReader: LowLevelDatabaseReader[CV]

  trait ReadContext {
    val copyInfo: CopyInfo
    val schema: ColumnIdMap[ColumnInfo]
    def withRows[A](cids: ColumnIdSet, offset: Option[Long], limit: Option[Long])(f: Iterator[ColumnIdMap[CV]] => A): A // TODO: I think this should return a Managed[Iterator...]
  }

  def openDataset(datasetName: String, copy: CopySelector): Managed[Option[ReadContext]]
}

object DatasetReader {
  private class Impl[CV](val databaseReader: LowLevelDatabaseReader[CV]) extends DatasetReader[CV] {
    class S(val copyInfo: CopyInfo, val schema: ColumnIdMap[ColumnInfo], llCtx: databaseReader.ReadContext) extends ReadContext {
      def withRows[A](keySet: ColumnIdSet, limit: Option[Long], offset: Option[Long])(f: Iterator[ColumnIdMap[CV]] => A): A =
        llCtx.withRows(copyInfo, schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system PK in this dataset?")), schema.filter { (id, _) => keySet.contains(id) }, f, limit, offset)
    }

    def openDataset(datasetName: String, copySelector: CopySelector): Managed[Option[ReadContext]] =
      new SimpleArm[Option[ReadContext]] {
        def flatMap[A](f: Option[ReadContext] => A): A = for {
          llCtx <- databaseReader.openDatabase
        } yield {
          val ctx = llCtx.loadDataset(datasetName, copySelector) map { case (initialCopy, initialSchema) =>
            new S(initialCopy, initialSchema, llCtx)
          }
          f(ctx)
        }
      }
  }

  def apply[CV](lowLevelReader: LowLevelDatabaseReader[CV]): DatasetReader[CV] = new Impl(lowLevelReader)
}

trait DatasetMutator[CT, CV] {
  val databaseMutator: LowLevelDatabaseMutator[CV]

  trait MutationContext {
    def copyInfo: CopyInfo
    def schema: ColumnIdMap[ColumnInfo]

    def schemaByLogicalName: Map[ColumnName, ColumnInfo] =
      schema.values.foldLeft(Map.empty[ColumnName, ColumnInfo]) { (acc, ci) =>
        acc + (ci.logicalName -> ci)
      }

    def addColumn(logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String): ColumnInfo
    def renameColumn(col: ColumnInfo, newName: ColumnName): ColumnInfo
    def makeSystemPrimaryKey(ci: ColumnInfo): ColumnInfo
    def makeUserPrimaryKey(ci: ColumnInfo): ColumnInfo
    def unmakeUserPrimaryKey(ci: ColumnInfo): ColumnInfo
    def dropColumn(ci: ColumnInfo)
    def truncate()

    sealed trait RowDataUpdateJob {
      def jobNumber: Int
    }
    case class DeleteJob(jobNumber: Int, id: CV) extends RowDataUpdateJob
    case class UpsertJob(jobNumber: Int, row: Row[CV]) extends RowDataUpdateJob

    def upsert(inputGenerator: Iterator[RowDataUpdateJob]): Report[CV]
  }

  type TrueMutationContext <: MutationContext

  def createDataset(as: String)(datasetName: String, tableBaseBase: String): Managed[Option[TrueMutationContext]]

  def openDataset(as: String)(datasetName: String): Managed[Option[TrueMutationContext]]

  sealed trait CopyContext
  case object DatasetDidNotExist extends CopyContext
  case class IncorrectLifecycleStage(lifecycleStage: LifecycleStage) extends CopyContext
  case class CopyOperationComplete(mutationContext: TrueMutationContext) extends CopyContext

  def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[CopyContext]
  def publishCopy(as: String)(datasetName: String, snapshotsToKeep: Option[Int]): Managed[CopyContext]
  def dropCopy(as: String)(datasetName: String): Managed[CopyContext]
}

object DatasetMutator {
  private class Impl[CT, CV](val databaseMutator: LowLevelDatabaseMutator[CV], typeNameFor: CT => TypeName, lockTimeout: Duration) extends DatasetMutator[CT, CV] {
    type TrueMutationContext = S
    class S(var copyInfo: CopyInfo, var _schema: ColumnIdMap[ColumnInfo], val schemaLoader: SchemaLoader, val logger: Logger[CV], llCtx: databaseMutator.MutationContext) extends MutationContext {
      var _schemaByLogicalName: Map[ColumnName, ColumnInfo] = null
      override def schemaByLogicalName = {
        if(_schemaByLogicalName == null) _schemaByLogicalName = super.schemaByLogicalName
        _schemaByLogicalName
      }

      def schema = _schema
      def schema_=(newSchema: ColumnIdMap[ColumnInfo]) = {
        _schemaByLogicalName = null
        _schema = newSchema
      }

      def now = llCtx.now
      def datasetMap = llCtx.datasetMap
      def datasetContetsCopier = llCtx.datasetContentsCopier(copyInfo.datasetInfo)

      def addColumn(logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String): ColumnInfo = {
        val newColumn = datasetMap.addColumn(copyInfo, logicalName, typeNameFor(typ), physicalColumnBaseBase)
        schemaLoader.addColumn(newColumn)
        schema += newColumn.systemId -> newColumn
        newColumn
      }

      def renameColumn(ci: ColumnInfo, newName: ColumnName): ColumnInfo = {
        val newCi = datasetMap.renameColumn(ci, newName)
        logger.logicalNameChanged(newCi)
        schema += newCi.systemId -> newCi
        newCi
      }

      def dropColumn(ci: ColumnInfo) {
        datasetMap.dropColumn(ci)
        schemaLoader.dropColumn(ci)
        schema -= ci.systemId
      }

      def makeSystemPrimaryKey(ci: ColumnInfo): ColumnInfo = {
        val result = datasetMap.setSystemPrimaryKey(ci)
        val ok = schemaLoader.makeSystemPrimaryKey(result)
        require(ok, "Column cannot be made a system primary key")
        schema += result.systemId -> result
        result
      }

      def unmakeUserPrimaryKey(ci: ColumnInfo): ColumnInfo = {
        val result = datasetMap.clearUserPrimaryKey(ci)
        val ok = schemaLoader.dropPrimaryKey(ci)
        require(ok, "Column cannot be unmade a system primary key")
        schema += result.systemId -> result
        result
      }

      def makeUserPrimaryKey(ci: ColumnInfo): ColumnInfo = {
        val result = datasetMap.setUserPrimaryKey(ci)
        val ok = schemaLoader.makePrimaryKey(result)
        require(ok, "Column cannot be made a primary key")
        schema += result.systemId -> result
        result
      }

      def datasetContentsCopier = llCtx.datasetContentsCopier(copyInfo.datasetInfo)

      def makeWorkingCopy(copyData: Boolean): CopyInfo = {
        val dataCopier = if(copyData) Some(datasetContentsCopier) else None
        datasetMap.ensureUnpublishedCopy(copyInfo.datasetInfo) match {
          case Left(_) =>
            sys.error("Already a working copy") // TODO: Better error
          case Right(CopyPair(oldCopy, newCopy)) =>
            assert(oldCopy == copyInfo)

            // Great.  Now we can actually do the data loading.
            schemaLoader.create(newCopy)
            val newSchema = datasetMap.schema(newCopy)
            for(ci <- newSchema.values) {
              schemaLoader.addColumn(ci)
            }

            dataCopier.foreach(_.copy(oldCopy, newCopy, schema))

            val finalSchema = new MutableColumnIdMap[ColumnInfo]
            for(ci <- newSchema.values) {
              val ci2 = if(schema(ci.systemId).isSystemPrimaryKey) {
                val pkified = datasetMap.setSystemPrimaryKey(ci)
                schemaLoader.makeSystemPrimaryKey(pkified)
                pkified
              } else {
                ci
              }

              val ci3 = if(schema(ci2.systemId).isUserPrimaryKey) {
                val pkified = datasetMap.setUserPrimaryKey(ci2)
                schemaLoader.makePrimaryKey(pkified)
                pkified
              } else {
                ci2
              }
              finalSchema(ci3.systemId) = ci3
            }

            copyInfo = newCopy
            schema = finalSchema.freeze()
            newCopy
        }
      }

      def publish(): CopyInfo = {
        val newCi = datasetMap.publish(copyInfo)
        logger.workingCopyPublished()
        copyInfo = newCi
        schema = datasetMap.schema(newCi)
        copyInfo
      }

      def truncate() {
        llCtx.truncate(copyInfo, logger)
      }

      def upsert(inputGenerator: Iterator[RowDataUpdateJob]): Report[CV] = {
        val (report, nextRowId, _) = llCtx.withDataLoader(copyInfo, schema, logger) { loader =>
          inputGenerator.foreach {
            case UpsertJob(jobNum, row) => loader.upsert(jobNum, row)
            case DeleteJob(jobNum, id) => loader.delete(jobNum, id)
          }
        }
        copyInfo = datasetMap.updateNextRowId(copyInfo, nextRowId)
        report
      }

      def drop() {
        datasetMap.dropCopy(copyInfo)
        schemaLoader.drop(copyInfo)
        copyInfo = datasetMap.latest(copyInfo.datasetInfo)
        schema = datasetMap.schema(copyInfo)
      }
    }

    private def go[A](username: String, datasetName: String, action: Option[S] => A): A =
      for {
        llCtx <- databaseMutator.openDatabase
      } yield {
        llCtx.datasetMap.datasetId(datasetName) match {
          case Some(datasetId) =>
            val ctx = llCtx.loadLatestVersionOfDataset(datasetId, lockTimeout) map { case (initialCopy, initialSchema) =>
              val logger = llCtx.logger(initialCopy.datasetInfo)
              val schemaLoader = llCtx.schemaLoader(initialCopy.datasetInfo)
              new S(initialCopy, initialSchema, schemaLoader, logger, llCtx)
            }
            val result = action(ctx)
            ctx.foreach { state =>
              llCtx.finishDatasetTransaction(username, state.copyInfo)
            }
            result
          case None =>
            action(None)
        }
      }

    def openDataset(as: String)(datasetName: String): Managed[Option[S]] = new SimpleArm[Option[S]] {
      def flatMap[A](f: Option[S] => A): A =
        go(as, datasetName, f)
    }

    def createDataset(as: String)(datasetName: String, tableBaseBase: String) = new SimpleArm[Option[S]] {
      def flatMap[A](f: Option[S] => A): A =
        for { llCtx <- databaseMutator.openDatabase } yield {
          val m = llCtx.datasetMap
          val firstVersion =
            try { m.create(datasetName, tableBaseBase) }
            catch { case _: DatasetAlreadyExistsException => return f(None) }
          val logger = llCtx.logger(firstVersion.datasetInfo)
          val schemaLoader = llCtx.schemaLoader(firstVersion.datasetInfo)
          schemaLoader.create(firstVersion)
          val state = new S(firstVersion, ColumnIdMap.empty, schemaLoader, logger, llCtx)
          val result = f(Some(state))
          llCtx.finishDatasetTransaction(as, state.copyInfo)
          result
        }
    }

    def firstOp[U](as: String, datasetName: String, targetStage: LifecycleStage, op: S => U) = new SimpleArm[CopyContext] {
      def flatMap[A](f: CopyContext => A): A =
        go(as,datasetName, {
          case None =>
            f(DatasetDidNotExist)
          case Some(ctx) =>
            if(ctx.copyInfo.lifecycleStage == targetStage) {
              op(ctx)
              f(CopyOperationComplete(ctx))
            } else {
              f(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage))
            }
        })
    }

    def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[CopyContext] =
      firstOp(as, datasetName, LifecycleStage.Published, _.makeWorkingCopy(copyData))

    def publishCopy(as: String)(datasetName: String, snapshotsToKeep: Option[Int]): Managed[CopyContext] =
      new SimpleArm[CopyContext] {
        def flatMap[B](f: CopyContext => B): B = {
          firstOp(as, datasetName, LifecycleStage.Unpublished, _.publish()).map {
            case good@CopyOperationComplete(ctx) =>
              for(count <- snapshotsToKeep) {
                val toDrop = ctx.datasetMap.snapshots(ctx.copyInfo.datasetInfo).dropRight(count)
                for(snapshot <- toDrop) {
                  ctx.datasetMap.dropCopy(snapshot)
                  ctx.schemaLoader.drop(snapshot)
                }
              }
              f(good)
            case noGood =>
              f(noGood)
          }
        }
      }

    def dropCopy(as: String)(datasetName: String): Managed[CopyContext] =
      firstOp(as, datasetName, LifecycleStage.Unpublished, _.drop())
  }

  def apply[CT, CV](lowLevelMutator: LowLevelDatabaseMutator[CV], typeNameFor: CT => TypeName, lockTimeout: Duration): DatasetMutator[CT, CV] =
    new Impl(lowLevelMutator, typeNameFor, lockTimeout)
}
