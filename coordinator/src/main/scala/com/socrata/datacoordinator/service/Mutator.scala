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
import scala.Some
import com.rojoma.json.ast.JString
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.datacoordinator.util.Counter

object Mutator {
  sealed abstract class StreamType {
    def index: Long
  }

  case class NormalMutation(index: Long) extends StreamType
  case class CreateDatasetMutation(index: Long, localeName: String) extends StreamType
  case class CreateWorkingCopyMutation(index: Long, copyData: Boolean) extends StreamType
  case class PublishWorkingCopyMutation(index: Long, keepingSnapshotCount: Option[Int]) extends StreamType
  case class DropWorkingCopyMutation(index: Long) extends StreamType

  sealed abstract class MutationException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause) {
    def index: Long
  }

  sealed abstract class InvalidCommandStreamException(msg: String = null, cause: Throwable = null) extends MutationException(msg, cause)
  case class EmptyCommandStream()(val index: Long) extends InvalidCommandStreamException
  case class CommandIsNotAnObject(value: JValue)(val index: Long) extends InvalidCommandStreamException
  case class MissingCommandField(value: JValue, field: String)(val index: Long) extends InvalidCommandStreamException
  case class InvalidCommandFieldValue(value: JValue, field: String)(val index: Long) extends InvalidCommandStreamException

  case class DatasetAlreadyExists(name: String)(val index: Long) extends MutationException
  case class NoSuchDataset(name: String)(val index: Long) extends MutationException
  case class CannotAcquireDatasetWriteLock(name: String)(val index: Long) extends MutationException
  case class IncorrectLifecycleStage(name: String, lifecycleStage: LifecycleStage)(val index: Long) extends MutationException
  case class IllegalColumnName(name: ColumnName)(val index: Long) extends MutationException
  case class NoSuchColumn(name: ColumnName)(val index: Long) extends MutationException
  case class NoSuchType(name: TypeName)(val index: Long) extends MutationException
  case class ColumnAlreadyExists(name: ColumnName)(val index: Long) extends MutationException
  case class PrimaryKeyAlreadyExists(name: ColumnName, existingName: ColumnName)(val index: Long) extends MutationException
  case class InvalidTypeForPrimaryKey(name: ColumnName, typ: TypeName)(val index: Long) extends MutationException
  case class NullsInColumn(name: ColumnName)(val index: Long) extends MutationException
  case class NotPrimaryKey(name: ColumnName)(val index: Long) extends MutationException
  case class DuplicateValuesInColumn(name: ColumnName)(val index: Long) extends MutationException
  case class InvalidSystemColumnOperation(name: ColumnName, op: String)(val index: Long) extends MutationException

  sealed abstract class RowDataException extends MutationException {
    def subindex: Int
  }
  case class InvalidUpsertCommand(value: JValue)(val index: Long, val subindex: Int) extends RowDataException
  case class InvalidValue(column: ColumnName, typ: TypeName, value: JValue)(val index: Long, val subindex: Int) extends RowDataException
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
          val json = fields.getOrElse(field, throw MissingCommandField(value, field)(index))
          JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(json, field)(index))
        }
        def getWithDefault[T : JsonCodec](field: String, default: T) = {
          fields.get(field) match {
            case Some(json) =>
              JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(json, field)(index))
            case None =>
              default
          }
        }
        def getOption[T : JsonCodec](field: String) =
          fields.get(field).map { json =>
            JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(json, field)(index))
          }
      })
    case other =>
      throw new CommandIsNotAnObject(other)(index)
  }

  sealed abstract class Command {
    def index: Long
  }
  case class AddColumn(index: Long, name: ColumnName, typ: TypeName) extends Command
  case class DropColumn(index: Long, name: ColumnName) extends Command
  case class RenameColumn(index: Long, from: ColumnName, to: ColumnName) extends Command
  case class SetRowId(index: Long, name: ColumnName) extends Command
  case class DropRowId(index: Long, name: ColumnName) extends Command
  case class RowData(index: Long, truncate: Boolean, mergeReplace: MergeReplace) extends Command

  val AddColumnOp = "add column"
  val DropColumnOp = "drop column"
  val RenameColumnOp = "rename column"
  val SetRowIdOp = "set row id"
  val DropRowIdOp = "drop row id"
  val RowDataOp = "row data"

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
        case AddColumnOp =>
          val name = get[String]("name")
          val typ = get[String]("type")
          AddColumn(index, ColumnName(name), TypeName(typ))
        case DropColumnOp =>
          val column = get[String]("column")
          DropColumn(index, ColumnName(column))
        case RenameColumnOp =>
          val from = get[String]("from")
          val to  =get[String]("to")
          RenameColumn(index, ColumnName(from), ColumnName(to))
        case SetRowIdOp =>
          val column = get[String]("column")
          SetRowId(index, ColumnName(column))
        case DropRowIdOp =>
          val column = get[String]("column")
          DropRowId(index, ColumnName(column))
        case RowDataOp =>
          val truncate = getWithDefault("truncate", false)
          val mergeReplace = getWithDefault[MergeReplace]("update", Merge)
          RowData(index, truncate, mergeReplace)
        case other =>
          throw InvalidCommandFieldValue(JString(other), "c")(index)
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
  def systemSchema: Map[ColumnName, CT]
  def systemIdColumnName: ColumnName
  def typeNameFor(typ: CT): TypeName
  def nameForTypeOpt(name: TypeName): Option[CT]
}

class Mutator[CT, CV](common: MutatorCommon[CT, CV]) {
  import Mutator._
  import common._

  case class UpsertError(failure: Failure[CV])(val index: Long) extends MutationException

  def createCommandStream(index: Long, value: JValue, remainingCommands: Iterator[JValue]) =
    withObjectFields(index, value) { accessor =>
      import accessor._
      val command = get[String]("c")
      val dataset = get[String]("dataset")
      val user = get[String]("user")
      val fatalRowErrors = getWithDefault("fatal_row_errors", true)
      val streamType = command match {
        case "create" =>
          val localeName = getWithDefault("locale", "en_US")
          // TODO: Validate and canonicalize locale name
          CreateDatasetMutation(index, localeName)
        case "copy" =>
          val copyData = get[Boolean]("copy_data")
          CreateWorkingCopyMutation(index, copyData)
        case "publish" =>
          val snapshotLimit = getOption[Int]("snapshot_limit")
          PublishWorkingCopyMutation(index, snapshotLimit)
        case "drop" =>
          DropWorkingCopyMutation(index)
        case "normal" =>
          NormalMutation(index)
        case other =>
          throw InvalidCommandFieldValue(JString(other), "c")(index)
      }
      new CommandStream(streamType, dataset, user, fatalRowErrors, remainingCommands.buffered)
    }

  def apply(u: Universe[CT, CV] with DatasetMutatorProvider, jsonRepFor: ColumnInfo[CT] => JsonColumnReadRep[CT, CV], commandStream: Iterator[JValue]) {
    if(commandStream.isEmpty) throw EmptyCommandStream()(0L)
    val commands = createCommandStream(0L, commandStream.next(), commandStream)
    def user = commands.user

    def doProcess(ctx: DatasetMutator[CT, CV]#MutationContext) = {
      val processor = new Processor(jsonRepFor)
      processor.carryOutCommands(ctx, commands)
    }

    def process(index: Long, datasetName: String, mutator: DatasetMutator[CT, CV])(maybeCtx: mutator.CopyContext) = maybeCtx match {
      case mutator.CopyOperationComplete(ctx) =>
        doProcess(ctx)
      case mutator.IncorrectLifecycleStage(stage) =>
        throw IncorrectLifecycleStage(datasetName, stage)(index)
      case mutator.DatasetDidNotExist =>
        throw NoSuchDataset(commands.datasetName)(index)
    }

    try {
      val mutator = u.datasetMutator
      commands.streamType match {
        case NormalMutation(idx) =>
          for(ctxOpt <- mutator.openDataset(user)(commands.datasetName)) {
            val ctx = ctxOpt.getOrElse { throw NoSuchDataset(commands.datasetName)(idx) }
            doProcess(ctx)
          }
        case CreateDatasetMutation(idx, localeName) =>
          for(ctxOpt <- u.datasetMutator.createDataset(user)(commands.datasetName, "t", localeName)) {
            val ctx = ctxOpt.getOrElse { throw DatasetAlreadyExists(commands.datasetName)(idx) }
            for((col, typ) <- systemSchema) {
              val ci = ctx.addColumn(col, typ, physicalColumnBaseBase(col, systemColumn = true))
              if(col == systemIdColumnName) {
                ctx.makeSystemPrimaryKey(ci)
              }
            }
            doProcess(ctx)
          }
        case CreateWorkingCopyMutation(idx, copyData) =>
          mutator.createCopy(user)(commands.datasetName, copyData = copyData).map(process(idx, commands.datasetName, mutator))
        case PublishWorkingCopyMutation(idx, keepingSnapshotCount) =>
          mutator.publishCopy(user)(commands.datasetName, keepingSnapshotCount).map(process(idx, commands.datasetName, mutator))
        case DropWorkingCopyMutation(idx) =>
          mutator.dropCopy(user)(commands.datasetName).map(process(idx, commands.datasetName, mutator))
      }
    } catch {
      case e: DatasetIdInUseByWriterException =>
        throw CannotAcquireDatasetWriteLock(e.datasetId)(commands.streamType.index)
    }
  }

  class Processor(jsonRepFor: ColumnInfo[CT] => JsonColumnReadRep[CT, CV]) {
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
      cmd match {
        case AddColumn(idx, name, typName) =>
          if(!isLegalLogicalName(name)) throw IllegalColumnName(name)(idx)
          mutator.columnInfo(name) match {
            case None =>
              val typ = nameForTypeOpt(typName).getOrElse {
                throw NoSuchType(typName)(idx)
              }
              mutator.addColumn(name, typ, physicalColumnBaseBase(name))
              None
            case Some(_) =>
              throw ColumnAlreadyExists(name)(idx)
          }
        case DropColumn(idx, name) =>
          mutator.columnInfo(name) match {
            case Some(colInfo) =>
              if(isSystemColumnName(name)) throw InvalidSystemColumnOperation(name, DropColumnOp)(idx)
              mutator.dropColumn(colInfo)
            case None =>
              throw NoSuchColumn(name)(idx)
          }
          None
        case RenameColumn(idx, from, to) =>
          mutator.columnInfo(from) match {
            case Some(colInfo) =>
              if(isSystemColumnName(from)) throw InvalidSystemColumnOperation(from, RenameColumnOp)(idx)
              if(!isLegalLogicalName(to)) throw IllegalColumnName(to)(idx)
              if(mutator.columnInfo(to).isDefined) throw ColumnAlreadyExists(to)(idx)
              mutator.renameColumn(colInfo, to)
            case None =>
              throw NoSuchColumn(from)(idx)
          }
          None
        case SetRowId(idx, name) =>
          mutator.columnInfo(name) match {
            case Some(colInfo) =>
              for(pkCol <- mutator.schema.values.find(_.isUserPrimaryKey))
                throw PrimaryKeyAlreadyExists(name, pkCol.logicalName)(idx)
              if(isSystemColumnName(name)) throw InvalidSystemColumnOperation(name, SetRowIdOp)(idx)
              try {
                mutator.makeUserPrimaryKey(colInfo)
              } catch {
                case e: mutator.PrimaryKeyCreationException => e match {
                  case mutator.UnPKableColumnException(_, _) =>
                    throw InvalidTypeForPrimaryKey(colInfo.logicalName, typeNameFor(colInfo.typ))(idx)
                  case mutator.NullCellsException(c) =>
                    throw NullsInColumn(colInfo.logicalName)(idx)
                  case mutator.DuplicateCellsException(_) =>
                    throw DuplicateValuesInColumn(colInfo.logicalName)(idx)
                }
              }
            case None =>
              throw NoSuchColumn(name)(idx)
          }
          None
        case DropRowId(idx, name) =>
          mutator.columnInfo(name) match {
            case Some(colInfo) =>
              if(!colInfo.isUserPrimaryKey) throw NotPrimaryKey(name)(idx)
              mutator.unmakeUserPrimaryKey(colInfo)
            case None =>
              throw NoSuchColumn(name)(idx)
          }
          None
        case RowData(idx, truncate, mergeReplace) =>
          if(mergeReplace == Replace) ??? // TODO: implement this
          if(truncate) mutator.truncate()
          Some(processRowData(idx, commands.rawCommandStream, commands.fatalRowErrors, mutator))
      }
    }

    def processRowData(idx: Long, rows: BufferedIterator[JValue], fatalRowErrors: Boolean, mutator: DatasetMutator[CT,CV]#MutationContext): Report[CV] = {
      import mutator._
      val plan = new RowDecodePlan(schema, jsonRepFor, typeNameFor)
      val counter = new Counter(1)
      try {
        val it = new Iterator[RowDataUpdateJob] {
          def hasNext = rows.hasNext && JNull != rows.head
          def next() = {
            if(!hasNext) throw new NoSuchElementException
            plan(rows.next()) match {
              case Right(row) => UpsertJob(counter(), row)
              case Left(id) => DeleteJob(counter(), id)
            }
          }
        }
        val result = mutator.upsert(it)
        if(rows.hasNext && JNull == rows.head) rows.next()
        if(fatalRowErrors && result.errors.nonEmpty) {
          throw UpsertError(result.errors.minBy(_._1)._2)(idx)
        }
        result
      } catch {
        case e: plan.BadDataException => e match {
          case plan.BadUpsertCommandException(value) =>
            throw InvalidUpsertCommand(value)(idx, counter.lastValue)
          case plan.UninterpretableFieldValue(column, value, columnType)  =>
            throw InvalidValue(column, typeNameFor(columnType), value)(idx, counter.lastValue)
        }
      }
    }
  }
}
