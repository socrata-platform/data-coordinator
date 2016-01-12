package com.socrata.datacoordinator
package truth

import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime
import com.rojoma.simplearm.{SimpleArm, Managed}

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id._
import scala.concurrent.duration.Duration

trait LowLevelDatabaseReader[CT, CV] {
  trait ReadContext {
    def datasetMap: DatasetMapReader[CT]

    def loadDataset(datasetId: DatasetId, latest: CopySelector): Option[DatasetCopyContext[CT]]
    def loadDataset(latest: CopyInfo): DatasetCopyContext[CT]

    def approximateRowCount(copyCtx: DatasetCopyContext[CT]): Long
    def rows(copyCtx: DatasetCopyContext[CT], sidCol: ColumnId, limit: Option[Long],
             offset: Option[Long], sorted: Boolean): Managed[Iterator[ColumnIdMap[CV]]]
  }

  def openDatabase: Managed[ReadContext]
}

trait LowLevelDatabaseMutator[CT, CV] {
  trait MutationContext {
    def now: DateTime
    def datasetMap: DatasetMapWriter[CT]
    def logger(info: DatasetInfo, user: String): Logger[CT, CV]
    def schemaLoader(logger: Logger[CT, CV]): SchemaLoader[CT]
    def datasetContentsCopier(logger: Logger[CT, CV]): DatasetContentsCopier[CT]
    def withDataLoader[A](copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV], reportWriter: ReportWriter[CV],
                          replaceUpdatedRows: Boolean)(f: Loader[CV] => A): (Long, A)
    def truncate(table: CopyInfo, logger: Logger[CT, CV]): Unit

    def finishDatasetTransaction(username: String, copyInfo: CopyInfo, updateLastUpdated: Boolean): Unit

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
    def copyInfo: CopyInfo = copyCtx.copyInfo
    def schema: ColumnIdMap[ColumnInfo[CT]] = copyCtx.schema
    def approximateRowCount: Long
    def rows(cids: ColumnIdSet = schema.keySet, limit: Option[Long] = None, offset: Option[Long] = None,
             sorted: Boolean = true): Managed[Iterator[ColumnIdMap[CV]]]
  }

  def openDataset(datasetId: DatasetId, copy: CopySelector): Managed[Option[ReadContext]]
  def openDataset(copy: CopyInfo): Managed[ReadContext]
}

object DatasetReader {
  private class Impl[CT, CV](val databaseReader: LowLevelDatabaseReader[CT, CV]) extends DatasetReader[CT, CV] {
    class S(val copyCtx: DatasetCopyContext[CT], llCtx: databaseReader.ReadContext) extends ReadContext {
      def approximateRowCount: Long = llCtx.approximateRowCount(copyCtx)

      def rows(keySet: ColumnIdSet, limit: Option[Long], offset: Option[Long],
               sorted: Boolean): Managed[Iterator[ColumnIdMap[CV]]] =
        llCtx.rows(copyCtx.verticalSlice { col => keySet.contains(col.systemId) }, copyCtx.pkCol_!.systemId,
                   limit = limit, offset = offset, sorted = sorted)
    }

    def openDataset(datasetId: DatasetId, copySelector: CopySelector): Managed[Option[ReadContext]] =
      new SimpleArm[Option[ReadContext]] {
        def flatMap[A](f: Option[ReadContext] => A): A = for {
          llCtx <- databaseReader.openDatabase
        } yield {
          val ctxOpt = llCtx.loadDataset(datasetId, copySelector).map(new S(_, llCtx))
          f(ctxOpt)
        }
      }

    def openDataset(copyInfo: CopyInfo): Managed[ReadContext] =
      new SimpleArm[ReadContext] {
        def flatMap[A](f: ReadContext => A): A = for {
          llCtx <- databaseReader.openDatabase
        } yield {
          val ctx = new S(llCtx.loadDataset(copyInfo), llCtx)
          f(ctx)
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
    def primaryKey: ColumnInfo[CT]
    def versionColumn: ColumnInfo[CT]
    def columnInfo(id: ColumnId): Option[ColumnInfo[CT]]
    def columnInfo(name: UserColumnId): Option[ColumnInfo[CT]]

    case class ColumnToAdd(userColumnId: UserColumnId, fieldName: Option[ColumnName], typ: CT, physicalColumnBaseBase: String, computationStrategyInfo: Option[ComputationStrategyInfo] = None)

    def addColumns(columns: Iterable[ColumnToAdd]): Iterable[ColumnInfo[CT]]
    def makeSystemPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]
    def makeVersion(ci: ColumnInfo[CT]): ColumnInfo[CT]

    /**
     * @throws PrimaryKeyCreationException
     */
    def makeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]
    sealed abstract class PrimaryKeyCreationException extends Exception
    case class UnPKableColumnException(name: UserColumnId, typ: CT) extends PrimaryKeyCreationException
    case class NullCellsException(name: UserColumnId) extends PrimaryKeyCreationException
    case class DuplicateCellsException(name: UserColumnId) extends PrimaryKeyCreationException

    def unmakeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]

    def dropColumns(columns: Iterable[ColumnInfo[CT]]): Unit
    def dropComputationStrategy(column: ColumnInfo[CT]): Unit
    def updateFieldName(column: ColumnInfo[CT], newName: ColumnName): Unit
    def truncate(): Unit

    sealed trait RowDataUpdateJob {
      def jobNumber: Int
    }
    case class DeleteJob(jobNumber: Int, id: CV, version: Option[Option[RowVersion]]) extends RowDataUpdateJob
    case class UpsertJob(jobNumber: Int, row: Row[CV]) extends RowDataUpdateJob

    def upsert(inputGenerator: Iterator[RowDataUpdateJob], reportWriter: ReportWriter[CV], replaceUpdatedRows: Boolean): Unit

    def createOrUpdateRollup(name: RollupName, soql: String): Unit
    def dropRollup(name: RollupName): Option[RollupInfo]
  }

  type TrueMutationContext <: MutationContext

  def createDataset(as: String)(localeName: String): Managed[TrueMutationContext]

  def openDataset(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[Option[TrueMutationContext]]

  sealed trait DropCopyContext
  sealed trait CopyContext
  sealed trait CopyContextError extends CopyContext
  case class DatasetDidNotExist() extends CopyContextError with DropCopyContext // for some reason Scala 2.10.2 thinks a match isn't exhaustive if this is a case object
  case class IncorrectLifecycleStage(currentLifecycleStage: LifecycleStage, expectedLifecycleStages: Set[LifecycleStage]) extends CopyContextError with DropCopyContext
  case class CopyOperationComplete(mutationContext: TrueMutationContext) extends CopyContext
  case object InitialWorkingCopy extends DropCopyContext
  case object DropComplete extends DropCopyContext

  def createCopy(as: String)(datasetId: DatasetId, copyData: Boolean, check: DatasetCopyContext[CT] => Unit): Managed[CopyContext]
  def publishCopy(as: String)(datasetId: DatasetId, snapshotsToKeep: Option[Int], check: DatasetCopyContext[CT] => Unit): Managed[CopyContext]
  def dropCopy(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[DropCopyContext]
}

object DatasetMutator {
  private class Impl[CT, CV](val databaseMutator: LowLevelDatabaseMutator[CT, CV], lockTimeout: Duration) extends DatasetMutator[CT, CV] {
    type TrueMutationContext = S
    class S(var copyCtx: MutableDatasetCopyContext[CT], llCtx: databaseMutator.MutationContext,
            logger: Logger[CT, CV], val schemaLoader: SchemaLoader[CT]) extends MutationContext {
      def copyInfo: CopyInfo = copyCtx.copyInfo
      def schema: ColumnIdMap[ColumnInfo[CT]] = copyCtx.currentSchema
      def primaryKey: ColumnInfo[CT] = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
        sys.error("No primary key on this dataset?")
      }
      def versionColumn: ColumnInfo[CT] = schema.values.find(_.isVersion).getOrElse {
        sys.error("No version column on this dataset?")
      }
      def columnInfo(id: ColumnId): Option[ColumnInfo[CT]] = copyCtx.columnInfoOpt(id)
      def columnInfo(name: UserColumnId): Option[ColumnInfo[CT]] = copyCtx.columnInfoOpt(name)

      var doingRows = false
      def checkDoingRows(): Unit = {
        if(doingRows) throw new IllegalStateException("Cannot perform operation while rows are being processed")
      }

      def now: DateTime = llCtx.now
      def datasetMap: DatasetMapWriter[CT] = llCtx.datasetMap

      def addColumns(columnsToAdd: Iterable[ColumnToAdd]): Iterable[ColumnInfo[CT]] = {
        checkDoingRows()
        val newColumns = columnsToAdd.toVector.map { cta =>
          val newColumn = datasetMap.addColumn(copyInfo, cta.userColumnId, cta.fieldName, cta.typ, cta.physicalColumnBaseBase, cta.computationStrategyInfo)
          copyCtx.addColumn(newColumn)
          newColumn
        }
        schemaLoader.addColumns(newColumns)
        newColumns
      }

      def dropColumns(columns: Iterable[ColumnInfo[CT]]): Unit = {
        checkDoingRows()
        val cs = columns.toVector
        for(ci <- cs) {
          datasetMap.dropColumn(ci)
          copyCtx.removeColumn(ci.systemId)
        }
        schemaLoader.dropColumns(cs)
      }

      def dropComputationStrategy(column: ColumnInfo[CT]): Unit = {
        val updated = datasetMap.dropComputationStrategy(column)
        copyCtx.updateColumn(updated)
        schemaLoader.dropComputationStrategy(updated)
      }

      def updateFieldName(column: ColumnInfo[CT], newName: ColumnName): Unit = {
        val updated = datasetMap.updateFieldName(column, newName)
        copyCtx.updateColumn(updated)
        schemaLoader.updateFieldName(updated)
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

      def makeVersion(ci: ColumnInfo[CT]): ColumnInfo[CT] = {
        checkDoingRows()
        val result = datasetMap.setVersion(ci)
        schemaLoader.makeVersion(result)
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

      def datasetContentsCopier: DatasetContentsCopier[CT] = llCtx.datasetContentsCopier(logger)

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
            schemaLoader.addColumns(newSchema.values)

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

              val maybeVersioncol = if(copyCtx.columnInfo(maybeUserIdCol.systemId).isVersion) {
                val verified = datasetMap.setVersion(maybeUserIdCol)
                schemaLoader.makeVersion(verified)
                verified
              } else {
                maybeUserIdCol
              }

              if(maybeVersioncol ne ci) newCopyCtx.addColumn(maybeVersioncol)
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

      def truncate(): Unit = {
        checkDoingRows()
        llCtx.truncate(copyInfo, logger)
      }

      def upsert(inputGenerator: Iterator[RowDataUpdateJob], reportWriter: ReportWriter[CV],
                 replaceUpdatedRows: Boolean): Unit = {
        checkDoingRows()
        try {
          doingRows = true
          val (nextCounterValue, _) = llCtx.withDataLoader(copyCtx.frozenCopy(), logger, reportWriter, replaceUpdatedRows) { loader =>
            inputGenerator.foreach {
              case UpsertJob(jobNum, row) => loader.upsert(jobNum, row)
              case DeleteJob(jobNum, id, ver) => loader.delete(jobNum, id, ver)
            }
          }
          copyCtx.copyInfo = datasetMap.updateNextCounterValue(copyInfo, nextCounterValue)
        } finally {
          doingRows = false
        }
      }

      def drop(): Unit = {
        datasetMap.dropCopy(copyInfo)
        schemaLoader.drop(copyInfo)
        // Do not update copyCtx.copyInfo or previously published dataVersion will be bumped
        // copyCtx.copyInfo = datasetMap.latest(copyInfo.datasetInfo)
      }

      def createOrUpdateRollup(name: RollupName, soql: String): Unit = {
        val info: RollupInfo = datasetMap.createOrUpdateRollup(copyInfo, name, soql)
        logger.rollupCreatedOrUpdated(info)
      }

      def dropRollup(name: RollupName): Option[RollupInfo] = {
        datasetMap.rollup(copyInfo, name) match {
          case r@Some(ru) =>
            datasetMap.dropRollup(copyInfo, Some(ru.name))
            logger.rollupDropped(ru)
            r
          case None =>
            None
        }
      }
    }

    sealed trait CanUpdateOccur
    case object UpdateCanOccur extends CanUpdateOccur
    case object UpdateCannotOccur extends CanUpdateOccur

    private def go[A](username: String, datasetId: DatasetId, canUpdateOccur: CanUpdateOccur, action: Option[S] => A): A =
      for {
        llCtx <- databaseMutator.openDatabase
      } yield {
        val ctx = llCtx.loadLatestVersionOfDataset(datasetId, lockTimeout) map { copyCtx =>
          val logger = llCtx.logger(copyCtx.datasetInfo, username)
          val schemaLoader = llCtx.schemaLoader(logger)
          new S(copyCtx.thaw(), llCtx, logger, schemaLoader)
        }
        val result = action(ctx)
        ctx.foreach { state =>
          llCtx.finishDatasetTransaction(username, state.copyInfo, updateLastUpdated = canUpdateOccur == UpdateCanOccur)
        }
        result
      }

    def openDataset(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[Option[S]] = new SimpleArm[Option[S]] {
      def flatMap[A](f: Option[S] => A): A =
        go(as, datasetId, UpdateCanOccur, {
          case None => f(None)
          case s@Some(ctx) =>
            check(new DatasetCopyContext[CT](ctx.copyInfo, ctx.schema))
            f(s)
        })
    }

    def createDataset(as: String)(localeName: String) = new SimpleArm[S] {
      def flatMap[A](f: S => A): A =
        for { llCtx <- databaseMutator.openDatabase } yield {
          val m = llCtx.datasetMap
          val firstVersion = m.create(localeName)
          val logger = llCtx.logger(firstVersion.datasetInfo, as)
          val schemaLoader = llCtx.schemaLoader(logger)
          schemaLoader.create(firstVersion)
          val state = new S(new DatasetCopyContext(firstVersion, ColumnIdMap.empty).thaw(), llCtx, logger, schemaLoader)
          val result = f(state)
          llCtx.finishDatasetTransaction(as, state.copyInfo, updateLastUpdated = true)
          result
        }
    }

    def firstOp[U](as: String, datasetId: DatasetId, targetStage: LifecycleStage, op: S => U,
                   check: DatasetCopyContext[CT] => Unit) = new SimpleArm[CopyContext] {
      def flatMap[A](f: CopyContext => A): A =
        go(as, datasetId, UpdateCanOccur, {
          case None =>
            f(DatasetDidNotExist())
          case Some(ctx) =>
            check(new DatasetCopyContext(ctx.copyInfo, ctx.schema))
            if(ctx.copyInfo.lifecycleStage == targetStage) {
              op(ctx)
              f(CopyOperationComplete(ctx))
            } else {
              f(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage, Set(targetStage)))
            }
        })
    }

    def createCopy(as: String)(datasetId: DatasetId, copyData: Boolean,
                               check: DatasetCopyContext[CT] => Unit): Managed[CopyContext] =
      firstOp(as, datasetId, LifecycleStage.Published, _.makeWorkingCopy(copyData), check)

    def publishCopy(as: String)(datasetId: DatasetId, snapshotsToKeep: Option[Int],
                                check: DatasetCopyContext[CT] => Unit): Managed[CopyContext] =
      new SimpleArm[CopyContext] {
        def flatMap[B](f: CopyContext => B): B = {
          firstOp(as, datasetId, LifecycleStage.Unpublished, _.publish(), check).map {
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

    def dropCopy(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[DropCopyContext] = new SimpleArm[DropCopyContext] {
      def flatMap[A](f: DropCopyContext => A): A =
        go(as, datasetId, UpdateCannotOccur, {
          case None =>
            f(DatasetDidNotExist())
          case Some(ctx) =>
            check(new DatasetCopyContext(ctx.copyInfo, ctx.schema))
            try {
              ctx.drop()
            } catch {
              case e: CopyInWrongStateForDropException =>
                return f(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage, e.acceptableStates))
              case _: CannotDropInitialWorkingCopyException =>
                return f(InitialWorkingCopy)
            }
            f(DropComplete)
        })
    }
  }

  def apply[CT, CV](lowLevelMutator: LowLevelDatabaseMutator[CT, CV], lockTimeout: Duration): DatasetMutator[CT, CV] =
    new Impl(lowLevelMutator, lockTimeout)
}
