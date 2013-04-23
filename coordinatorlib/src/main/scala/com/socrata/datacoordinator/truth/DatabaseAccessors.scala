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
import com.socrata.datacoordinator.id.{ColumnId, DatasetId, RowId}
import scala.concurrent.duration.Duration
import com.socrata.soql.environment.{TypeName, ColumnName}

trait LowLevelDatabaseReader[CT, CV] {
  trait ReadContext {
    def datasetMap: DatasetMapReader[CT]

    def loadDataset(datasetName: String, latest: CopySelector): Option[DatasetCopyContext[CT]]

    def withRows[A](copyCtx: DatasetCopyContext[CT], sidCol: ColumnId, f: Iterator[ColumnIdMap[CV]] => A, limit: Option[Long], offset: Option[Long]): A
  }

  def openDatabase: Managed[ReadContext]
}

trait LowLevelDatabaseMutator[CT, CV] {
  trait MutationContext {
    def now: DateTime
    def datasetMap: DatasetMapWriter[CT]
    def logger(info: DatasetInfo): Logger[CT, CV]
    def schemaLoader(logger: Logger[CT, CV]): SchemaLoader[CT]
    def datasetContentsCopier(logger: Logger[CT, CV]): DatasetContentsCopier[CT]
    def withDataLoader[A](copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV])(f: Loader[CV] => A): (Report[CV], RowId, A)
    def truncate(table: CopyInfo, logger: Logger[CT, CV])

    def globalLog: GlobalLog

    def finishDatasetTransaction(username: String, copyInfo: CopyInfo)

    def loadLatestVersionOfDataset(datasetId: DatasetId, lockTimeout: Duration): Option[DatasetCopyContext[CT]]
  }

  def openDatabase: Managed[MutationContext]
}

sealed abstract class CopySelector
case object LatestCopy extends CopySelector
case object PublishedCopy extends CopySelector
case object WorkingCopy extends CopySelector
case class Snapshot(nth: Int) extends CopySelector

trait DatasetReader[CT, CV] {
  val databaseReader: LowLevelDatabaseReader[CT, CV]

  trait ReadContext {
    val copyCtx: DatasetCopyContext[CT]
    def copyInfo = copyCtx.copyInfo
    def schema = copyCtx.schema
    def withRows[A](cids: ColumnIdSet, offset: Option[Long], limit: Option[Long])(f: Iterator[ColumnIdMap[CV]] => A): A // TODO: I think this should return a Managed[Iterator...]
  }

  def openDataset(datasetName: String, copy: CopySelector): Managed[Option[ReadContext]]
}

object DatasetReader {
  private class Impl[CT, CV](val databaseReader: LowLevelDatabaseReader[CT, CV]) extends DatasetReader[CT, CV] {
    class S(val copyCtx: DatasetCopyContext[CT], llCtx: databaseReader.ReadContext) extends ReadContext {
      def withRows[A](keySet: ColumnIdSet, limit: Option[Long], offset: Option[Long])(f: Iterator[ColumnIdMap[CV]] => A): A =
        llCtx.withRows(copyCtx.verticalSlice { col => keySet.contains(col.systemId) }, schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system PK in this dataset?")).systemId, f, limit, offset)
    }

    def openDataset(datasetName: String, copySelector: CopySelector): Managed[Option[ReadContext]] =
      new SimpleArm[Option[ReadContext]] {
        def flatMap[A](f: Option[ReadContext] => A): A = for {
          llCtx <- databaseReader.openDatabase
        } yield {
          val ctxOpt = llCtx.loadDataset(datasetName, copySelector).map(new S(_, llCtx))
          f(ctxOpt)
        }
      }
  }

  def apply[CT, CV](lowLevelReader: LowLevelDatabaseReader[CT, CV]): DatasetReader[CT, CV] = new Impl(lowLevelReader)
}

trait DatasetMutator[CT, CV] {
  val databaseMutator: LowLevelDatabaseMutator[CT, CV]

  trait MutationContext {
    def copyInfo: CopyInfo
    def schema: ColumnIdMap[ColumnInfo[CT]]
    def columnInfo(id: ColumnId): Option[ColumnInfo[CT]]
    def columnInfo(name: ColumnName): Option[ColumnInfo[CT]]

    def addColumn(logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String): ColumnInfo[CT]
    def renameColumn(col: ColumnInfo[CT], newName: ColumnName): ColumnInfo[CT]
    def makeSystemPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]

    /**
     * @throws PrimaryKeyCreationException
     */
    def makeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]
    sealed abstract class PrimaryKeyCreationException extends Exception
    case class UnPKableColumnException(name: ColumnName, typ: CT) extends PrimaryKeyCreationException
    case class NullCellsException(name: ColumnName) extends PrimaryKeyCreationException
    case class DuplicateCellsException(name: ColumnName) extends PrimaryKeyCreationException

    def unmakeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]
    def dropColumn(ci: ColumnInfo[CT])
    def truncate()

    sealed trait RowDataUpdateJob {
      def jobNumber: Int
    }
    case class DeleteJob(jobNumber: Int, id: CV) extends RowDataUpdateJob
    case class UpsertJob(jobNumber: Int, row: Row[CV]) extends RowDataUpdateJob

    def upsert(inputGenerator: Iterator[RowDataUpdateJob]): Report[CV]
  }

  type TrueMutationContext <: MutationContext

  def createDataset(as: String)(datasetName: String, tableBaseBase: String, localeName: String): Managed[Option[TrueMutationContext]]

  def openDataset(as: String)(datasetName: String): Managed[Option[TrueMutationContext]]

  sealed trait DropCopyContext
  sealed trait CopyContext extends DropCopyContext
  case object DatasetDidNotExist extends CopyContext
  case class IncorrectLifecycleStage(currentLifecycleStage: LifecycleStage, expectedLifecycleStages: Set[LifecycleStage]) extends CopyContext
  case class CopyOperationComplete(mutationContext: TrueMutationContext) extends CopyContext
  case object InitialWorkingCopy extends DropCopyContext

  def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[CopyContext]
  def publishCopy(as: String)(datasetName: String, snapshotsToKeep: Option[Int]): Managed[CopyContext]
  def dropCopy(as: String)(datasetName: String): Managed[DropCopyContext]
}

object DatasetMutator {
  private class Impl[CT, CV](val databaseMutator: LowLevelDatabaseMutator[CT, CV], lockTimeout: Duration) extends DatasetMutator[CT, CV] {
    type TrueMutationContext = S
    class S(var copyCtx: MutableDatasetCopyContext[CT], llCtx: databaseMutator.MutationContext, logger: Logger[CT, CV], val schemaLoader: SchemaLoader[CT]) extends MutationContext {
      def copyInfo = copyCtx.copyInfo
      def schema = copyCtx.currentSchema
      def columnInfo(id: ColumnId) = copyCtx.columnInfoOpt(id)
      def columnInfo(name: ColumnName) = copyCtx.columnInfoOpt(name)

      def datasetContetsCopier = llCtx.datasetContentsCopier(logger)

      var doingRows = false
      def checkDoingRows() {
        if(doingRows) throw new IllegalStateException("Cannot perform operation while rows are being processed")
      }

      def now = llCtx.now
      def datasetMap = llCtx.datasetMap

      def addColumn(logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String): ColumnInfo[CT] = {
        checkDoingRows()
        val newColumn = datasetMap.addColumn(copyInfo, logicalName, typ, physicalColumnBaseBase)
        schemaLoader.addColumn(newColumn)
        copyCtx.addColumn(newColumn)
        newColumn
      }

      def renameColumn(ci: ColumnInfo[CT], newName: ColumnName): ColumnInfo[CT] = {
        checkDoingRows()
        val newCi = datasetMap.renameColumn(ci, newName)
        logger.logicalNameChanged(newCi)
        copyCtx.addColumn(newCi)
        newCi
      }

      def dropColumn(ci: ColumnInfo[CT]) {
        checkDoingRows()
        datasetMap.dropColumn(ci)
        schemaLoader.dropColumn(ci)
        copyCtx.removeColumn(ci.systemId)
      }

      def makeSystemPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT] = {
        checkDoingRows()
        val result = datasetMap.setSystemPrimaryKey(ci)
        try {
          schemaLoader.makeSystemPrimaryKey(result)
        } catch {
          case e: schemaLoader.PrimaryKeyCreationException =>
            sys.error("Unable to create system primary key? " + e)
        }
        copyCtx.addColumn(result)
        result
      }

      def unmakeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT] = {
        checkDoingRows()
        val result = datasetMap.clearUserPrimaryKey(ci)
        schemaLoader.dropPrimaryKey(ci)
        copyCtx.addColumn(result)
        result
      }

      def makeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT] = {
        checkDoingRows()
        val result = datasetMap.setUserPrimaryKey(ci)
        try {
          schemaLoader.makePrimaryKey(result)
        } catch {
          case e: schemaLoader.PrimaryKeyCreationException => e match {
            case schemaLoader.NotPKableType(col, typ) =>
              throw UnPKableColumnException(col, typ)
            case schemaLoader.NullValuesInColumn(col) =>
              throw NullCellsException(col)
            case schemaLoader.DuplicateValuesInColumn(col) =>
              throw DuplicateCellsException(col)
          }
        }
        copyCtx.addColumn(result)
        result
      }

      def datasetContentsCopier = llCtx.datasetContentsCopier(logger)

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

            val newFrozenCopyContext = new DatasetCopyContext(newCopy, newSchema)
            dataCopier.foreach(_.copy(oldCopy, newFrozenCopyContext))
            val newCopyCtx = newFrozenCopyContext.thaw()

            for(ci <- newCopyCtx.currentColumns) {
              val maybeSystemIdCol = if(copyCtx.columnInfo(ci.systemId).isSystemPrimaryKey) {
                val pkified = datasetMap.setSystemPrimaryKey(ci)
                schemaLoader.makeSystemPrimaryKey(pkified)
                newCopyCtx.addColumn(pkified)
                pkified
              } else {
                ci
              }

              val maybeUserIdCol = if(copyCtx.columnInfo(maybeSystemIdCol.systemId).isUserPrimaryKey) {
                val pkified = datasetMap.setUserPrimaryKey(maybeSystemIdCol)
                schemaLoader.makePrimaryKey(pkified)
                pkified
              } else {
                maybeSystemIdCol
              }

              if(maybeUserIdCol ne ci) newCopyCtx.addColumn(maybeUserIdCol)
            }

            copyCtx = newCopyCtx
            newCopy
        }
      }

      def publish(): CopyInfo = {
        val newCi = datasetMap.publish(copyInfo)
        logger.workingCopyPublished()
        copyCtx = new DatasetCopyContext(newCi, datasetMap.schema(newCi)).thaw()
        copyInfo
      }

      def truncate() {
        checkDoingRows()
        llCtx.truncate(copyInfo, logger)
      }

      def upsert(inputGenerator: Iterator[RowDataUpdateJob]): Report[CV] = {
        checkDoingRows()
        try {
          doingRows = true
          val (report, nextRowId, _) = llCtx.withDataLoader(copyCtx.frozenCopy(), logger) { loader =>
            inputGenerator.foreach {
              case UpsertJob(jobNum, row) => loader.upsert(jobNum, row)
              case DeleteJob(jobNum, id) => loader.delete(jobNum, id)
            }
          }
          copyCtx.copyInfo = datasetMap.updateNextRowId(copyInfo, nextRowId)
          report
        } finally {
          doingRows = false
        }
      }

      def drop() {
        datasetMap.dropCopy(copyInfo)
        schemaLoader.drop(copyInfo)
        copyCtx.copyInfo = datasetMap.latest(copyInfo.datasetInfo)
      }
    }

    private def go[A](username: String, datasetName: String, action: Option[S] => A): A =
      for {
        llCtx <- databaseMutator.openDatabase
      } yield {
        llCtx.datasetMap.datasetId(datasetName) match {
          case Some(datasetId) =>
            val ctx = llCtx.loadLatestVersionOfDataset(datasetId, lockTimeout) map { copyCtx =>
              val logger = llCtx.logger(copyCtx.datasetInfo)
              val schemaLoader = llCtx.schemaLoader(logger)
              new S(copyCtx.thaw(), llCtx, logger, schemaLoader)
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

    def createDataset(as: String)(datasetName: String, tableBaseBase: String, localeName: String) = new SimpleArm[Option[S]] {
      def flatMap[A](f: Option[S] => A): A =
        for { llCtx <- databaseMutator.openDatabase } yield {
          val m = llCtx.datasetMap
          val firstVersion =
            try { m.create(datasetName, tableBaseBase, localeName) }
            catch { case _: DatasetAlreadyExistsException => return f(None) }
          val logger = llCtx.logger(firstVersion.datasetInfo)
          val schemaLoader = llCtx.schemaLoader(logger)
          schemaLoader.create(firstVersion)
          val state = new S(new DatasetCopyContext(firstVersion, ColumnIdMap.empty).thaw(), llCtx, logger, schemaLoader)
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
              f(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage, Set(targetStage)))
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

    def dropCopy(as: String)(datasetName: String): Managed[DropCopyContext] = new SimpleArm[DropCopyContext] {
      def flatMap[A](f: DropCopyContext => A): A =
        go(as, datasetName, {
          case None =>
            f(DatasetDidNotExist)
          case Some(ctx) =>
            try {
              ctx.drop()
            } catch {
              case e: CopyInWrongStateForDropException =>
                return f(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage, e.acceptableStates))
              case _: CannotDropInitialWorkingCopyException =>
                return f(InitialWorkingCopy)
            }
            f(CopyOperationComplete(ctx))
        })
    }
  }

  def apply[CT, CV](lowLevelMutator: LowLevelDatabaseMutator[CT, CV], lockTimeout: Duration): DatasetMutator[CT, CV] =
    new Impl(lowLevelMutator, lockTimeout)
}
