package com.socrata.datacoordinator
package service

import com.rojoma.json.ast._
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.truth.{DatasetIdInUseByWriterException, DatasetMutator}
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage, DatasetInfo, AbstractColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.loader.{Failure, Report}
import scala.collection.immutable.VectorBuilder
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.ColumnId
import scala.Some
import com.rojoma.json.ast.JString
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.datacoordinator.util.Counter

object Mutator {
  sealed abstract class StreamType

  case object NormalMutation extends StreamType
  case object CreateDatasetMutation extends StreamType
  case class CreateWorkingCopyMutation(copyData: Boolean) extends StreamType
  case class PublishWorkingCopyMutation(keepingSnapshotCount: Option[Long]) extends StreamType
  case object DropWorkingCopyMutation extends StreamType

  sealed abstract class MutationException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause)

  sealed abstract class InvalidCommandStreamException(msg: String = null, cause: Throwable = null) extends MutationException(msg, cause)
  case class EmptyCommandStream() extends InvalidCommandStreamException
  case class CommandIsNotAnObject(index: Long, value: JValue) extends InvalidCommandStreamException
  case class MissingCommandField(index: Long, value: JValue, field: String) extends InvalidCommandStreamException
  case class InvalidCommandFieldValue(index: Long, value: JValue, field: String) extends InvalidCommandStreamException

  case class NoSuchDataset(name: String) extends MutationException
  case class DatasetAlreadyExists(name: String) extends MutationException
  case class IncorrectLifecycleStage(name: String, lifecycleStage: LifecycleStage) extends MutationException
  case class LockTimeout(name: String) extends MutationException
  // UpsertError is defined inside the Mutator class

  sealed abstract class MergeReplace
  object MergeReplace {
    implicit val jCodec = new JsonCodec[MergeReplace] {
      val MergeS = JString("merge")
      val ReplaceS = JString("replace")
      def encode(x: MergeReplace): JValue = x match {
        case Merge => MergeS
        case Replace => ReplaceS
      }
      def decode(x: JValue): Option[MergeReplace] = x match {
        case MergeS => Some(Merge)
        case ReplaceS => Some(Replace)
        case _ => None
      }
    }
  }
  case object Merge extends MergeReplace
  case object Replace extends MergeReplace

  trait Accessor {
    def fields: scala.collection.Map[String, JValue]
    def get[T: JsonCodec](field: String): T
    def getOption[T: JsonCodec](field: String): Option[T]
    def getWithDefault[T: JsonCodec](field: String, default: T): T
  }

  def withObjectFields[A](index: Long, value: JValue)(f: Accessor => A): A = value match {
    case JObject(rawFields) =>
      f(new Accessor {
        val fields = rawFields
        def get[T : JsonCodec](field: String) = {
          val json = fields.getOrElse(field, throw MissingCommandField(index, value, field))
          JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(index, json, field))
        }
        def getWithDefault[T : JsonCodec](field: String, default: T) = {
          fields.get(field) match {
            case Some(json) =>
              JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(index, json, field))
            case None =>
              default
          }
        }
        def getOption[T : JsonCodec](field: String) =
          fields.get(field).map { json =>
            JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(index, json, field))
          }
      })
    case other =>
      throw new CommandIsNotAnObject(index, other)
  }

  sealed abstract class Command
  case class AddColumn(name: ColumnName, typ: TypeName) extends Command
  case class DropColumn(name: ColumnName) extends Command
  case class RenameColumn(from: ColumnName, to: ColumnName) extends Command
  case class SetRowId(name: ColumnName) extends Command
  case class DropRowId(name: ColumnName) extends Command
  case class RowData(truncate: Boolean, mergeReplace: MergeReplace) extends Command

  class CommandStream(val streamType: StreamType, val datasetName: String, val user: String, val fatalRowErrors: Boolean, val rawCommandStream: BufferedIterator[JValue]) {
    private var idx = 1L
    private def nextIdx() = {
      val res = idx
      idx += 1
      res
    }

    private def decodeCommand(index: Long, json: JValue): Command = withObjectFields(index, json) { accessor =>
      import accessor._

      get[String]("c") match {
        case "add column" =>
          val name = get[String]("name")
          val typ = get[String]("type")
          AddColumn(ColumnName(name), TypeName(typ))
        case "drop column" =>
          val column = get[String]("column")
          DropColumn(ColumnName(column))
        case "rename column" =>
          val from = get[String]("from")
          val to  =get[String]("to")
          RenameColumn(ColumnName(from), ColumnName(to))
        case "set row id" =>
          val column = get[String]("column")
          SetRowId(ColumnName(column))
        case "drop row id" =>
          val column = get[String]("column")
          DropRowId(ColumnName(column))
        case "row data" =>
          val truncate = getWithDefault("truncate", false)
          val mergeReplace = getWithDefault[MergeReplace]("update", Merge)
          RowData(truncate, mergeReplace)
        case other =>
          throw new InvalidCommandFieldValue(index, JString(other), "c")
      }
    }

    def nextCommand() =
      if(rawCommandStream.hasNext) Some(decodeCommand(nextIdx(), rawCommandStream.next()))
      else None
  }
}

trait MutatorCommon[CT, CV] {
  def physicalColumnBaseBase(logicalColumnName: ColumnName, systemColumn: Boolean = false): String
  def isLegalLogicalName(identifier: ColumnName): Boolean
  def isSystemColumnName(identifier: ColumnName): Boolean
  def systemSchema: Map[ColumnName, TypeName]
  def systemIdColumnName: ColumnName
  def typeNameFor(typ: CT): TypeName
  def nameForTypeOpt(name: TypeName): Option[CT]
}

class Mutator[CT, CV](common: MutatorCommon[CT, CV]) {
  import Mutator._
  import common._

  case class UpsertError(failure: Failure[CV]) extends MutationException

  def createCommandStream(value: JValue, remainingCommands: Iterator[JValue]) =
    withObjectFields(0, value) { accessor =>
      import accessor._
      val command = get[String]("c")
      val dataset = get[String]("dataset")
      val user = get[String]("user")
      val fatalRowErrors = getWithDefault("fatal_row_errors", true)
      val streamType = command match {
        case "create" =>
          CreateDatasetMutation
        case "copy" =>
          val copyData = get[Boolean]("copy_data")
          CreateWorkingCopyMutation(copyData)
        case "publish" =>
          val snapshotLimit = getOption[Long]("snapshot_limit")
          PublishWorkingCopyMutation(snapshotLimit)
        case "drop" =>
          DropWorkingCopyMutation
        case "normal" =>
          NormalMutation
        case other =>
          throw new InvalidCommandFieldValue(0, JString(other), "c")
      }
      new CommandStream(streamType, dataset, user, fatalRowErrors, remainingCommands.buffered)
    }

  private def lookupType(typeName: TypeName): CT =
    nameForTypeOpt(typeName) match {
      case Some(typ) => typ
      case None => ??? // TODO: Proper error
    }

  def apply(u: Universe[CT, CV] with DatasetMutatorProvider, jsonRepFor: DatasetInfo => AbstractColumnInfoLike => JsonColumnReadRep[CT, CV], commandStream: Iterator[JValue]) {
    if(commandStream.isEmpty) throw EmptyCommandStream()
    val commands = createCommandStream(commandStream.next(), commandStream)
    def user = commands.user

    def doProcess(ctx: DatasetMutator[CT, CV]#MutationContext) = {
      val processor = new Processor(jsonRepFor(ctx.copyInfo.datasetInfo))
      processor.carryOutCommands(ctx, commands)
    }

    def process(datasetName: String, mutator: DatasetMutator[CT, CV])(maybeCtx: mutator.CopyContext) = maybeCtx match {
      case mutator.CopyOperationComplete(ctx) =>
        doProcess(ctx)
      case mutator.IncorrectLifecycleStage(stage) =>
        throw new IncorrectLifecycleStage(datasetName, stage)
      case mutator.DatasetDidNotExist =>
        throw new NoSuchDataset(commands.datasetName)
    }

    try {
      val mutator = u.datasetMutator
      commands.streamType match {
        case NormalMutation =>
          for(ctxOpt <- mutator.openDataset(user)(commands.datasetName)) {
            val ctx = ctxOpt.getOrElse { throw new NoSuchDataset(commands.datasetName) }
            doProcess(ctx)
          }
        case CreateDatasetMutation =>
          for(ctxOpt <- u.datasetMutator.createDataset(user)(commands.datasetName, "t")) {
            val ctx = ctxOpt.getOrElse { throw new DatasetAlreadyExists(commands.datasetName) }
            for((col, typ) <- systemSchema) {
              val ci = ctx.addColumn(col, lookupType(typ), physicalColumnBaseBase(col, systemColumn = true))
              if(col == systemIdColumnName) {
                ctx.makeSystemPrimaryKey(ci)
              }
            }
            doProcess(ctx)
          }
        case CreateWorkingCopyMutation(copyData) =>
          mutator.createCopy(user)(commands.datasetName, copyData = copyData).map(process(commands.datasetName, mutator))
        case PublishWorkingCopyMutation(keepingSnapshotCount) =>
          mutator.publishCopy(user)(commands.datasetName).map(process(commands.datasetName, mutator))
        case DropWorkingCopyMutation =>
          mutator.dropCopy(user)(commands.datasetName).map(process(commands.datasetName, mutator))
      }
    } catch {
      case e: DatasetIdInUseByWriterException =>
        throw new LockTimeout(e.datasetId)
    }
  }

  class Processor(jsonRepFor: AbstractColumnInfoLike => JsonColumnReadRep[CT, CV]) {
    def carryOutCommands(mutator: DatasetMutator[CT, CV]#MutationContext, commands: CommandStream): Seq[Report[CV]] = {
      val reports = new VectorBuilder[Report[CV]]
      def loop() {
        commands.nextCommand() match {
          case Some(cmd) => reports ++= carryOutCommand(mutator, commands, cmd); loop()
          case None => /* done */
        }
      }
      loop()
      reports.result()
    }

    def carryOutCommand(mutator: DatasetMutator[CT, CV]#MutationContext, commands: CommandStream, cmd: Command): Option[Report[CV]] = {
      import mutator._
      cmd match {
        case AddColumn(name, typ) =>
          if(!isLegalLogicalName(name)) ??? // TODO: proper error
          if(schemaByLogicalName.contains(name)) ??? // TODO: proper error
          addColumn(name, lookupType(typ), physicalColumnBaseBase(name))
          None
        case DropColumn(name) =>
          schemaByLogicalName.get(name) match {
            case Some(colInfo) => dropColumn(colInfo)
            case None => ??? // TODO: proper error
          }
          None
        case RenameColumn(from, to) =>
          schemaByLogicalName.get(from) match {
            case Some(colInfo) =>
              if(isSystemColumnName(from)) ??? // TODO: proper error
              if(!isLegalLogicalName(to)) ??? // TODO: proper error
              if(schemaByLogicalName.contains(to)) ??? // TODO: proper error
              renameColumn(colInfo, to)
            case None =>
              ??? // TODO: proper error
          }
          None
        case SetRowId(name) =>
          schemaByLogicalName.get(name) match {
            case Some(colInfo) =>
              if(isSystemColumnName(name)) ??? // TODO: proper error
              if(schema.values.exists(_.isUserPrimaryKey)) ??? // TODO: proper error
              makeUserPrimaryKey(colInfo) // TODO: errors for "unPKable type" and "data has nulls or dups"
            case None =>
              ??? // TODO: proper error
          }
          None
        case DropRowId(name) =>
          schemaByLogicalName.get(name) match {
            case Some(colInfo) =>
              if(!colInfo.isUserPrimaryKey) ??? // TODO: proper error
              unmakeUserPrimaryKey(colInfo)
            case None =>
              ??? // TODO: proper error
          }
          None
        case RowData(truncate, mergeReplace) =>
          if(mergeReplace == Replace) ??? // TODO: implement this
          if(truncate) mutator.truncate()
          Some(processRowData(commands.rawCommandStream, commands.fatalRowErrors, mutator))
      }
    }

    def processRowData(rows: BufferedIterator[JValue], fatalRowErrors: Boolean, mutator: DatasetMutator[CT,CV]#MutationContext): Report[CV] = {
      import mutator._
      val plan = new RowDecodePlan(schema, jsonRepFor, typeNameFor)
      try {
        val counter = new Counter(1)
        val it = new Iterator[RowDataUpdateJob] {
          def hasNext = rows.hasNext && JNull != rows.head
          def next() = {
            if(!hasNext) throw new NoSuchElementException
            plan(rows.next) match {
              case Right(row) => UpsertJob(counter(), row)
              case Left(id) => DeleteJob(counter(), id)
            }
          }
        }
        val result = mutator.upsert(it)
        if(rows.hasNext && JNull == rows.head) rows.next()
        if(fatalRowErrors && result.errors.nonEmpty) {
          throw UpsertError(result.errors.minBy(_._1)._2)
        }
        result
      } catch {
        case e: plan.BadDataException =>
          ??? // TODO: proper error
      }
    }
  }
}
