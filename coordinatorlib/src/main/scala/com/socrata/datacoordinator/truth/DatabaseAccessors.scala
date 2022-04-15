package com.socrata.datacoordinator
package truth

import com.rojoma.json.v3.ast.{JNumber, JObject, JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.util.CopyContextResult
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import org.joda.time.DateTime
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.{ColumnIdMap, ColumnIdSet}
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id._
import com.socrata.soql.ast.{JoinFunc, JoinQuery, JoinTable, Select}
import com.socrata.soql.{BinaryTree, Compound, Leaf, SoQLAnalyzer}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.exceptions.{NoSuchColumn, SoQLException}
import com.socrata.soql.parsing.{AbstractParser, Parser}

import scala.concurrent.duration.Duration

trait LowLevelDatabaseReader[CT, CV] {
  trait ReadContext {
    def datasetMap: DatasetMapReader[CT]

    def loadDataset(datasetId: DatasetId, latest: CopySelector): CopyContextResult[DatasetCopyContext[CT]]
    def loadDataset(latest: CopyInfo): DatasetCopyContext[CT]

    def approximateRowCount(copyCtx: DatasetCopyContext[CT]): Long
    def rows(copyCtx: DatasetCopyContext[CT],
             sidCol: ColumnId,
             limit: Option[Long],
             offset: Option[Long], sorted: Boolean,
             rowId: Option[CV] = None): Managed[Iterator[ColumnIdMap[CV]]]
  }

  def openDatabase: Managed[ReadContext]
}

trait LowLevelDatabaseMutator[CT, CV] {
  trait MutationContext {
    def now: DateTime
    def datasetMap: DatasetMapWriter[CT]
    def datasetMapReader: DatasetMapReader[CT]
    def logger(info: DatasetInfo, user: String): Logger[CT, CV]
    def schemaLoader(logger: Logger[CT, CV]): SchemaLoader[CT]
    def datasetContentsCopier(logger: Logger[CT, CV]): DatasetContentsCopier[CT]
    def withDataLoader[A](copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV], reportWriter: ReportWriter[CV],
                          replaceUpdatedRows: Boolean, updateOnly: Boolean)(f: Loader[CV] => A): (Long, A)
    def truncate(table: CopyInfo, logger: Logger[CT, CV]): Unit

    def finishDatasetTransaction(username: String, copyInfo: CopyInfo, updateLastUpdated: Boolean, dataShapeUpdated: Boolean): Unit

    def loadLatestVersionOfDataset(datasetId: DatasetId, lockTimeout: Duration): Option[DatasetCopyContext[CT]]

    def outOfDateFeedbackSecondaries(datasetId: DatasetId): Set[String]
  }

  def openDatabase: Managed[MutationContext]
}

sealed abstract class CopySelector
object CopySelector {
  implicit val enc = new JsonEncode[CopySelector] {
    override def encode(x: CopySelector): JValue = x match {
      case LatestCopy => JString("latest")
      case PublishedCopy => JString("published")
      case WorkingCopy => JString("working")
      case Snapshot(n) => JNumber(n)
    }
  }
}
case object LatestCopy extends CopySelector
case object PublishedCopy extends CopySelector
case object WorkingCopy extends CopySelector
case class Snapshot(copyNumber: Long) extends CopySelector

trait DatasetReader[CT, CV] {
  val databaseReader: LowLevelDatabaseReader[CT, CV]

  trait ReadContext {
    val copyCtx: DatasetCopyContext[CT]
    def copyInfo: CopyInfo = copyCtx.copyInfo
    def schema: ColumnIdMap[ColumnInfo[CT]] = copyCtx.schema
    def approximateRowCount: Long
    def rows(cids: ColumnIdSet = schema.keySet,
             limit: Option[Long] = None,
             offset: Option[Long] = None,
             sorted: Boolean = true,
             rowId: Option[CV] = None): Managed[Iterator[ColumnIdMap[CV]]]
  }

  def openDataset(datasetId: DatasetId, copy: CopySelector): Managed[CopyContextResult[ReadContext]]
  def openDataset(copy: CopyInfo): Managed[ReadContext]
}

object DatasetReader {
  private class Impl[CT, CV](val databaseReader: LowLevelDatabaseReader[CT, CV]) extends DatasetReader[CT, CV] {
    class S(val copyCtx: DatasetCopyContext[CT], llCtx: databaseReader.ReadContext) extends ReadContext {
      def approximateRowCount: Long = llCtx.approximateRowCount(copyCtx)

      def rows(keySet: ColumnIdSet,
               limit: Option[Long],
               offset: Option[Long],
               sorted: Boolean,
               rowId: Option[CV]): Managed[Iterator[ColumnIdMap[CV]]] =
        llCtx.rows(copyCtx.verticalSlice { col => keySet.contains(col.systemId) },
                   copyCtx.pkCol_!.systemId,
                   limit = limit,
                   offset = offset,
                   sorted = sorted,
                   rowId = rowId)
    }

    def openDataset(datasetId: DatasetId, copySelector: CopySelector): Managed[CopyContextResult[ReadContext]] =
      new Managed[CopyContextResult[ReadContext]] {
        def run[A](f: CopyContextResult[ReadContext] => A): A = for {
          llCtx <- databaseReader.openDatabase
        } {
          val ctxOpt = llCtx.loadDataset(datasetId, copySelector).map(new S(_, llCtx))
          f(ctxOpt)
        }
      }

    def openDataset(copyInfo: CopyInfo): Managed[ReadContext] =
      new Managed[ReadContext] {
        def run[A](f: ReadContext => A): A = for {
          llCtx <- databaseReader.openDatabase
        } {
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
    def systemId: ColumnInfo[CT]
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
    case class RollupValidationException(ru: RollupInfo, message: String) extends Exception(message)

    def unmakeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT]

    def dropColumns(columns: Iterable[ColumnInfo[CT]]): Unit

    def addComputationStrategy(column: ColumnInfo[CT], cs: ComputationStrategyInfo): Unit
    def dropComputationStrategy(column: ColumnInfo[CT]): Unit

    def updateFieldName(column: ColumnInfo[CT], newName: ColumnName): Unit
    def truncate(): Unit

    sealed trait RowDataUpdateJob {
      def jobNumber: Int
    }
    case class DeleteJob(jobNumber: Int, id: CV, version: Option[Option[RowVersion]]) extends RowDataUpdateJob
    case class UpsertJob(jobNumber: Int, row: Row[CV]) extends RowDataUpdateJob

    def upsert(inputGenerator: Iterator[RowDataUpdateJob], reportWriter: ReportWriter[CV], replaceUpdatedRows: Boolean, updateOnly: Boolean, bySystemId: Boolean): Unit

    def createOrUpdateRollup(name: RollupName, soql: String, rawSoql: Option[String]): Either[Exception, RollupInfo]
    def dropRollup(name: RollupName): Option[RollupInfo]
    def secondaryReindex(): Unit
    def createOrUpdateIndexDirective(column: ColumnInfo[CT], directive: JObject): Unit
    def dropIndexDirective(column: ColumnInfo[CT]): Unit
  }

  type TrueMutationContext <: MutationContext

  def createDataset(as: String)(localeName: String, resourceName: Option[String]): Managed[TrueMutationContext]

  def openDataset(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[Option[TrueMutationContext]]

  sealed trait DropCopyContext
  sealed trait CopyContext
  sealed trait CopyContextError extends CopyContext
  case class SecondariesNotUpToDate(secondaries: Set[String]) extends CopyContextError with DropCopyContext
  case class DatasetDidNotExist() extends CopyContextError with DropCopyContext // for some reason Scala 2.10.2 thinks a match isn't exhaustive if this is a case object
  case class IncorrectLifecycleStage(currentLifecycleStage: LifecycleStage, expectedLifecycleStages: Set[LifecycleStage]) extends CopyContextError with DropCopyContext
  case class CopyOperationComplete(mutationContext: TrueMutationContext) extends CopyContext
  case object InitialWorkingCopy extends DropCopyContext
  case object DropComplete extends DropCopyContext

  def createCopy(as: String)(datasetId: DatasetId, copyData: Boolean, check: DatasetCopyContext[CT] => Unit): Managed[CopyContext]
  def publishCopy(as: String)(datasetId: DatasetId, keepSnapshot: Boolean, check: DatasetCopyContext[CT] => Unit): Managed[CopyContext]
  def dropCopy(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[DropCopyContext]
}

object DatasetMutator {
  private class Impl[CT, CV](val databaseMutator: LowLevelDatabaseMutator[CT, CV], lockTimeout: Duration, soqlAnalyzer: => SoQLAnalyzer[CT]) extends DatasetMutator[CT, CV] {
    type TrueMutationContext = S

    private val log = org.slf4j.LoggerFactory.getLogger(classOf[Impl[_, _]])

    class S(var copyCtx: MutableDatasetCopyContext[CT], llCtx: databaseMutator.MutationContext,
            logger: Logger[CT, CV], val schemaLoader: SchemaLoader[CT]) extends MutationContext {
      def copyInfo: CopyInfo = copyCtx.copyInfo
      def schema: ColumnIdMap[ColumnInfo[CT]] = copyCtx.currentSchema
      def systemId: ColumnInfo[CT] = schema.values.find(_.isSystemPrimaryKey).getOrElse {
        sys.error("No system id column on this dataset?")
      }
      def primaryKey: ColumnInfo[CT] = schema.values.find(_.isUserPrimaryKey).getOrElse(systemId)
      def versionColumn: ColumnInfo[CT] = schema.values.find(_.isVersion).getOrElse {
        sys.error("No version column on this dataset?")
      }
      def columnInfo(id: ColumnId): Option[ColumnInfo[CT]] = copyCtx.columnInfoOpt(id)
      def columnInfo(name: UserColumnId): Option[ColumnInfo[CT]] = copyCtx.columnInfoOpt(name)

      // This becomes true if a change is made which affects the
      // schema, the rows, or the meaning of future upsert operations.
      var dataShapeUpdated = false

      var doingRows = false
      def checkDoingRows(): Unit = {
        if(doingRows) throw new IllegalStateException("Cannot perform operation while rows are being processed")
      }

      def now: DateTime = llCtx.now
      def datasetMap: DatasetMapWriter[CT] = llCtx.datasetMap
      def datasetMapReader: DatasetMapReader[CT] = llCtx.datasetMapReader

      def addColumns(columnsToAdd: Iterable[ColumnToAdd]): Iterable[ColumnInfo[CT]] = {
        checkDoingRows()
        val newColumns = columnsToAdd.toVector.map { cta =>
          val newColumn = datasetMap.addColumn(copyInfo, cta.userColumnId, cta.fieldName, cta.typ, cta.physicalColumnBaseBase, cta.computationStrategyInfo)
          copyCtx.addColumn(newColumn)
          newColumn
        }
        schemaLoader.addColumns(newColumns)
        dataShapeUpdated = true
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
        dataShapeUpdated = true
      }

      def addComputationStrategy(column: ColumnInfo[CT], cs: ComputationStrategyInfo): Unit = {
        val updated = datasetMap.addComputationStrategy(column, cs)
        copyCtx.updateColumn(updated)
        schemaLoader.addComputationStrategy(updated, cs)
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
        dataShapeUpdated = true
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
        dataShapeUpdated = true
        result
      }

      def makeVersion(ci: ColumnInfo[CT]): ColumnInfo[CT] = {
        checkDoingRows()
        val result = datasetMap.setVersion(ci)
        schemaLoader.makeVersion(result)
        copyCtx.addColumn(result)
        dataShapeUpdated = true
        result
      }

      def unmakeUserPrimaryKey(ci: ColumnInfo[CT]): ColumnInfo[CT] = {
        checkDoingRows()
        val result = datasetMap.clearUserPrimaryKey(ci)
        schemaLoader.dropPrimaryKey(ci)
        copyCtx.addColumn(result)
        dataShapeUpdated = true
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
        dataShapeUpdated = true
        result
      }

      def datasetContentsCopier: DatasetContentsCopier[CT] = llCtx.datasetContentsCopier(logger)

      def checkFeedbackSecondaries(): Option[SecondariesNotUpToDate] = {
        val ood = llCtx.outOfDateFeedbackSecondaries(copyInfo.datasetInfo.systemId)
        if(ood.nonEmpty) Some(SecondariesNotUpToDate(ood))
        else None
      }

      def makeWorkingCopy(copyData: Boolean): Either[CopyContextError, CopyInfo] = {
        checkFeedbackSecondaries().toLeft {
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

              dataShapeUpdated ||= !copyData

              newCopy
          }
        }
      }

      def publish(keepSnapshot: Boolean): Either[CopyContextError, CopyInfo] = {
        checkFeedbackSecondaries().toLeft {
          val (newCi, snapshotCI) = datasetMap.publish(copyInfo)
          logger.workingCopyPublished()
          snapshotCI.foreach { sci =>
            if(keepSnapshot) {
              logger.snapshotDropped(sci) // no matter what, tell the secondaries the snapshot was dropped
            } else {
              datasetMap.dropCopy(sci)
              schemaLoader.drop(sci)
            }
          }
          copyCtx = new DatasetCopyContext(newCi, datasetMap.schema(newCi)).thaw()
          dropInvalidRollups(newCi)
          copyInfo
        }
      }

      private def dropInvalidRollups(copyInfo: CopyInfo): Unit = {
        val rollups = datasetMap.rollups(copyInfo)
        if (rollups.nonEmpty) {
          rollups.foreach { ru: RollupInfo =>
            try {
              validateRollup(ru)
            } catch {
              case ex: RollupValidationException =>
                log.info(s"drop invalid rollup ${ru.name.underlying} ${ex.getMessage}")
                dropRollup(ru.name)
            }
          }
        }
      }

      private def getPrefixedDsContext(copyInfo: CopyInfo): com.socrata.soql.environment.DatasetContext[CT] = {
        val prefixedSchema: Seq[(ColumnName, CT)] = datasetMap.schema(copyInfo).values.map { colInfo =>
          val prefix = if (colInfo.userColumnId.underlying.startsWith(":")) "" else "_"
          (new ColumnName(prefix + colInfo.userColumnId.underlying), colInfo.typ)
        }(collection.breakOut)
        val prefixedDsContext = new com.socrata.soql.environment.DatasetContext[CT] {
          val schema: OrderedMap[ColumnName, CT] = OrderedMap(prefixedSchema: _*)
        }
        prefixedDsContext
      }

      def validateRollup(ru: RollupInfo): Unit = {
        val prefixedDsContext = getPrefixedDsContext(ru.copyInfo)
        val analyzer = soqlAnalyzer
        try {
          val parsed = new Parser(AbstractParser.defaultParameters).binaryTreeSelect(ru.soql)
          val tableNames = collectTableNames(parsed)
          val initContext = Map(TableName.PrimaryTable.qualifier -> prefixedDsContext)
          val contexts = tableNames.foldLeft(initContext) { (acc, tn) =>
            val tableName = TableName(tn)
            val ctx = for {
              dsInfo <- datasetMapReader.datasetInfoByResourceName(ResourceName(tableName.nameWithSodaFountainPrefix))
              copyInfo <- datasetMap.published(dsInfo)
            } yield {
              getPrefixedDsContext(copyInfo)
            }
            ctx match {
              case Some(context) =>
                acc + (tn -> context)
              case None =>
                acc
            }
          }
          analyzer.analyzeBinary(parsed)(contexts)
        } catch {
          case ex: NoSuchColumn =>
            throw RollupValidationException(ru, ex.getMessage)
          case ex: SoQLException =>
            throw RollupValidationException(ru, ex.getMessage)
        }
      }

      def truncate(): Unit = {
        checkDoingRows()
        llCtx.truncate(copyInfo, logger)
        dataShapeUpdated = true
      }

      def upsert(inputGenerator: Iterator[RowDataUpdateJob], reportWriter: ReportWriter[CV],
                 replaceUpdatedRows: Boolean, updateOnly: Boolean, bySystemId: Boolean): Unit = {
        checkDoingRows()
        try {
          doingRows = true
          val (nextCounterValue, _) = llCtx.withDataLoader(copyCtx.frozenCopy(), logger, reportWriter, replaceUpdatedRows, updateOnly) { loader =>
            inputGenerator.foreach {
              case UpsertJob(jobNum, row) => loader.upsert(jobNum, row, bySystemId)
              case DeleteJob(jobNum, id, ver) => loader.delete(jobNum, id, ver, bySystemId)
            }
          }
          copyCtx.copyInfo = datasetMap.updateNextCounterValue(copyInfo, nextCounterValue)
        } finally {
          doingRows = false
          dataShapeUpdated = true // Almost certainly going to be reverted, but if it's not changes may have happened
        }
      }

      def drop(): Either[CopyContextError with DropCopyContext, Unit] = {
        checkFeedbackSecondaries().toLeft {
          datasetMap.dropCopy(copyInfo)
          schemaLoader.drop(copyInfo)
          dataShapeUpdated = true // Unfortunate, but we're reverting arbitrary versions so conservatively assume the worse
          // Do not update copyCtx.copyInfo or previously published dataVersion will be bumped
          // copyCtx.copyInfo = datasetMap.latest(copyInfo.datasetInfo)
        }
      }

      def createOrUpdateRollup(name: RollupName, soql: String, rawSoql: Option[String]): Either[Exception, RollupInfo] = {
        val info: RollupInfo = datasetMap.createOrUpdateRollup(copyInfo, name, soql, rawSoql)
        try {
          validateRollup(info)
          logger.rollupCreatedOrUpdated(info)
          Right(info)
        } catch {
          case ex: RollupValidationException =>
            log.info(s"create rollup failed ${name.underlying} ${ex.getMessage}")
            Left(ex)
        }
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

      def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
        selects match {
          case Compound(_, l, r) =>
            collectTableNames(l) ++ collectTableNames(r)
          case Leaf(select) =>
            select.joins.foldLeft(select.from.map(_.name).filter(_ != TableName.This).toSet) { (acc, join) =>
              join.from match {
                case JoinTable(TableName(name, _)) =>
                  acc + name
                case JoinQuery(selects, _) =>
                  acc ++ collectTableNames(selects)
                case JoinFunc(_, _) =>
                  throw new Exception("Unexpected join function")
              }
            }
        }
      }

      def secondaryReindex(): Unit = {
        logger.secondaryReindex()
      }

      def createOrUpdateIndexDirective(column: ColumnInfo[CT], directive: JObject): Unit = {
        datasetMap.createOrUpdateIndexDirective(column, directive)
        logger.indexDirectiveCreatedOrUpdated(column, directive)
      }

      def dropIndexDirective(column: ColumnInfo[CT]): Unit = {
        datasetMap.dropIndexDirective(column)
        logger.indexDirectiveDropped(column)
      }
    }

    sealed trait CanUpdateOccur
    case object UpdateCanOccur extends CanUpdateOccur
    case object UpdateCannotOccur extends CanUpdateOccur

    private def go[A](username: String, datasetId: DatasetId, canUpdateOccur: CanUpdateOccur, action: Option[S] => A): A =
      for {
        llCtx <- databaseMutator.openDatabase
      } {
        val ctx = llCtx.loadLatestVersionOfDataset(datasetId, lockTimeout) map { copyCtx =>
          val logger = llCtx.logger(copyCtx.datasetInfo, username)
          val schemaLoader = llCtx.schemaLoader(logger)
          new S(copyCtx.thaw(), llCtx, logger, schemaLoader)
        }
        val result = action(ctx)
        ctx.foreach { state =>
          llCtx.finishDatasetTransaction(username, state.copyInfo, updateLastUpdated = canUpdateOccur == UpdateCanOccur, dataShapeUpdated = state.dataShapeUpdated)
        }
        result
      }

    def openDataset(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[Option[S]] = new Managed[Option[S]] {
      def run[A](f: Option[S] => A): A =
        go(as, datasetId, UpdateCanOccur, {
          case None => f(None)
          case s@Some(ctx) =>
            check(new DatasetCopyContext[CT](ctx.copyInfo, ctx.schema))
            f(s)
        })
    }

    def createDataset(as: String)(localeName: String, resourceName: Option[String]) = new Managed[S] {
      def run[A](f: S => A): A =
        for { llCtx <- databaseMutator.openDatabase } {
          val m = llCtx.datasetMap
          val firstVersion = m.create(localeName, resourceName)
          val logger = llCtx.logger(firstVersion.datasetInfo, as)
          val schemaLoader = llCtx.schemaLoader(logger)
          schemaLoader.create(firstVersion)
          val state = new S(new DatasetCopyContext(firstVersion, ColumnIdMap.empty).thaw(), llCtx, logger, schemaLoader)
          val result = f(state)
          llCtx.finishDatasetTransaction(as, state.copyInfo, updateLastUpdated = true, dataShapeUpdated = state.dataShapeUpdated)
          result
        }
    }

    def firstOp[U](as: String, datasetId: DatasetId, targetStage: LifecycleStage, op: S => Either[CopyContextError, U],
                   check: DatasetCopyContext[CT] => Unit) = new Managed[CopyContext] {
      def run[A](f: CopyContext => A): A =
        go(as, datasetId, UpdateCanOccur, {
          case None =>
            f(DatasetDidNotExist())
          case Some(ctx) =>
            check(new DatasetCopyContext(ctx.copyInfo, ctx.schema))
            if(ctx.copyInfo.lifecycleStage == targetStage) {
              op(ctx) match {
                case Left(err) =>
                  f(err)
                case Right(_) =>
                  f(CopyOperationComplete(ctx))
              }
            } else {
              f(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage, Set(targetStage)))
            }
        })
    }

    def createCopy(as: String)(datasetId: DatasetId, copyData: Boolean,
                               check: DatasetCopyContext[CT] => Unit): Managed[CopyContext] =
      firstOp(as, datasetId, LifecycleStage.Published, _.makeWorkingCopy(copyData), check)

    def publishCopy(as: String)(datasetId: DatasetId, keepSnapshot: Boolean, check: DatasetCopyContext[CT] => Unit): Managed[CopyContext] =
      new Managed[CopyContext] {
        def run[B](f: CopyContext => B): B = {
          firstOp(as, datasetId, LifecycleStage.Unpublished, _.publish(keepSnapshot), check).run {
            case good@CopyOperationComplete(ctx) =>
              f(good)
            case noGood =>
              f(noGood)
          }
        }
      }

    def dropCopy(as: String)(datasetId: DatasetId, check: DatasetCopyContext[CT] => Unit): Managed[DropCopyContext] = new Managed[DropCopyContext] {
      def run[A](f: DropCopyContext => A): A =
        go(as, datasetId, UpdateCannotOccur, {
          case None =>
            f(DatasetDidNotExist())
          case Some(ctx) =>
            check(new DatasetCopyContext(ctx.copyInfo, ctx.schema))
            val dropResult =
              try {
                ctx.drop()
              } catch {
                case e: CopyInWrongStateForDropException =>
                  Left(IncorrectLifecycleStage(ctx.copyInfo.lifecycleStage, e.acceptableStates))
                case _: CannotDropInitialWorkingCopyException =>
                  Left(InitialWorkingCopy)
              }
            dropResult match {
              case Left(err) =>
                f(err)
              case Right(_) =>
                f(DropComplete)
            }
        })
    }
  }

  def apply[CT, CV](lowLevelMutator: LowLevelDatabaseMutator[CT, CV], lockTimeout: Duration, soqlAnalyzer: => SoQLAnalyzer[CT]): DatasetMutator[CT, CV] =
    new Impl(lowLevelMutator, lockTimeout, soqlAnalyzer)
}
