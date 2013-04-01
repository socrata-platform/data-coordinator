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

    def loadDataset(datasetName: String, latest: Boolean): Option[(CopyInfo, ColumnIdMap[ColumnInfo])]

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

trait DatasetReader[CV] {
  val databaseReader: LowLevelDatabaseReader[CV]

  trait ReadContext {
    val copyInfo: CopyInfo
    val schema: ColumnIdMap[ColumnInfo]
    def withRows[A](cids: ColumnIdSet, offset: Option[Long], limit: Option[Long])(f: Iterator[ColumnIdMap[CV]] => A): A // TODO: I think this should return a Managed[Iterator...]
  }

  /**
   * @param latest If false, this action operates on the published version even if there
   *               is a newer working copy.
   */
  def openDataset(datasetName: String, latest: Boolean): Managed[Option[ReadContext]]
}

object DatasetReader {
  private class Impl[CV](val databaseReader: LowLevelDatabaseReader[CV]) extends DatasetReader[CV] {
    class S(val copyInfo: CopyInfo, val schema: ColumnIdMap[ColumnInfo], llCtx: databaseReader.ReadContext) extends ReadContext {
      def withRows[A](keySet: ColumnIdSet, limit: Option[Long], offset: Option[Long])(f: Iterator[ColumnIdMap[CV]] => A): A =
        llCtx.withRows(copyInfo, schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system PK in this dataset?")), schema.filter { (id, _) => keySet.contains(id) }, f, limit, offset)
    }

    def openDataset(datasetName: String, latest: Boolean): Managed[Option[ReadContext]] =
      new SimpleArm[Option[ReadContext]] {
        def flatMap[A](f: Option[ReadContext] => A): A = for {
          llCtx <- databaseReader.openDatabase
        } yield {
          val ctx = llCtx.loadDataset(datasetName, latest) map { case (initialCopy, initialSchema) =>
            new S(initialCopy, initialSchema, llCtx)
          }
          f(ctx)
        }
      }
  }

  def apply[CV](lowLevelReader: LowLevelDatabaseReader[CV]): DatasetReader[CV] = new Impl(lowLevelReader)
}

trait DatasetMutator[CV] {
  val databaseMutator: LowLevelDatabaseMutator[CV]

  trait MutationContext {
    def copyInfo: CopyInfo
    def schema: ColumnIdMap[ColumnInfo]

    def schemaByLogicalName: Map[ColumnName, ColumnInfo] =
      schema.values.foldLeft(Map.empty[ColumnName, ColumnInfo]) { (acc, ci) =>
        acc + (ci.logicalName -> ci)
      }

    def addColumn(logicalName: ColumnName, typeName: TypeName, physicalColumnBaseBase: String): ColumnInfo
    def renameColumn(col: ColumnInfo, newName: ColumnName): ColumnInfo
    def makeSystemPrimaryKey(ci: ColumnInfo): ColumnInfo
    def makeUserPrimaryKey(ci: ColumnInfo): ColumnInfo
    def unmakeUserPrimaryKey(ci: ColumnInfo): ColumnInfo
    def dropColumn(ci: ColumnInfo)
    def truncate()
    def upsert(inputGenerator: Iterator[Either[CV, Row[CV]]]): Report[CV]
  }

  // FIXME: There is no way to tell whether the dataset exists before calling this
  def createDataset(as: String)(datasetName: String, tableBaseBase: String): Managed[MutationContext]

  def openDataset(as: String)(datasetName: String): Managed[Option[MutationContext]]

  // FIXME: There is no way to tell whether the dataset is in the right state for these before calling them.
  // So its return type should include all three of "this dataset did not exist", "it was in the wrong state",
  // and "ok, here's your MutationContext for more work."
  def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[Option[MutationContext]]
  def publishCopy(as: String)(datasetName: String): Managed[Option[MutationContext]]
  def dropCopy(as: String)(datasetName: String): Managed[Option[MutationContext]]
}

object DatasetMutator {
  private class Impl[CV](val databaseMutator: LowLevelDatabaseMutator[CV], lockTimeout: Duration) extends DatasetMutator[CV] {
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

      def addColumn(logicalName: ColumnName, typeName: TypeName, physicalColumnBaseBase: String): ColumnInfo = {
        val newColumn = datasetMap.addColumn(copyInfo, logicalName, typeName, physicalColumnBaseBase)
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

      def upsert(inputGenerator: Iterator[Either[CV, Row[CV]]]): Report[CV] = {
        val (report, nextRowId, _) = llCtx.withDataLoader(copyInfo, schema, logger) { loader =>
          inputGenerator.foreach {
            case Right(row) => loader.upsert(row)
            case Left(id) => loader.delete(id)
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

    def openDataset(as: String)(datasetName: String): Managed[Option[MutationContext]] = new SimpleArm[Option[MutationContext]] {
      def flatMap[A](f: Option[MutationContext] => A): A =
        go(as, datasetName, f)
    }

    def createDataset(as: String)(datasetName: String, tableBaseBase: String): Managed[MutationContext] = new SimpleArm[MutationContext] {
      def flatMap[A](f: MutationContext => A): A =
        for { llCtx <- databaseMutator.openDatabase } yield {
          val m = llCtx.datasetMap
          val firstVersion = m.create(datasetName, tableBaseBase)
          val logger = llCtx.logger(firstVersion.datasetInfo)
          val schemaLoader = llCtx.schemaLoader(firstVersion.datasetInfo)
          schemaLoader.create(firstVersion)
          val state = new S(firstVersion, ColumnIdMap.empty, schemaLoader, logger, llCtx)
          val result = f(state)
          llCtx.finishDatasetTransaction(as, state.copyInfo)
          result
        }
    }

    def firstOp[U](as: String, datasetName: String, op: S => U) = new SimpleArm[Option[MutationContext]] {
      def flatMap[A](f: Option[MutationContext] => A): A =
        go(as,datasetName, { ctxOpt =>
          ctxOpt.foreach(op)
          f(ctxOpt)
        })
    }

    def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[Option[MutationContext]] =
      firstOp(as, datasetName, _.makeWorkingCopy(copyData))

    def publishCopy(as: String)(datasetName: String): Managed[Option[MutationContext]] =
      firstOp(as, datasetName, _.publish())

    def dropCopy(as: String)(datasetName: String): Managed[Option[MutationContext]] =
      firstOp(as, datasetName, _.drop())
  }

  def apply[CV](lowLevelMutator: LowLevelDatabaseMutator[CV], lockTimeout: Duration): DatasetMutator[CV] =
    new Impl(lowLevelMutator, lockTimeout)
}
