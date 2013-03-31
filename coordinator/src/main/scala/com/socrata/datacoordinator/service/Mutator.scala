package com.socrata.datacoordinator
package service

import com.rojoma.json.ast._
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.loader.Report
import scala.collection.immutable.VectorBuilder
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.ColumnId
import scala.Some
import com.rojoma.json.ast.JString

object Mutator {
  sealed abstract class StreamType

  case object NormalMutation extends StreamType
  case object CreateDatasetMutation extends StreamType
  case class CreateWorkingCopyMutation(copyData: Boolean) extends StreamType
  case class PublishWorkingCopyMutation(keepingSnapshotCount: Option[Long]) extends StreamType
  case object DropWorkingCopyMutation extends StreamType

  sealed abstract class InvalidCommandStreamException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause)
  case class EmptyCommandStream() extends InvalidCommandStreamException
  case class CommandIsNotAnObject(index: Long, value: JValue) extends InvalidCommandStreamException
  case class MissingCommandField(index: Long, value: JValue, field: String) extends InvalidCommandStreamException
  case class InvalidCommandFieldValue(index: Long, value: JValue, field: String) extends InvalidCommandStreamException

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
  case class AddColumn(name: String, typ: String) extends Command
  case class DropColumn(name: String) extends Command
  case class RenameColumn(from: String, to: String) extends Command
  case class SetRowId(name: String) extends Command
  case class DropRowId(name: String) extends Command
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
          AddColumn(name, typ)
        case "drop column" =>
          val column = get[String]("column")
          DropColumn(column)
        case "rename column" =>
          val from = get[String]("from")
          val to  =get[String]("to")
          RenameColumn(from, to)
        case "set row id" =>
          val column = get[String]("column")
          SetRowId(column)
        case "drop row id" =>
          val column = get[String]("column")
          DropRowId(column)
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
  def repFor: AbstractColumnInfoLike =>  JsonColumnReadRep[CT, CV]
  def physicalColumnBaseBase(logicalColumnName: String, systemColumn: Boolean = false): String
  def isLegalLogicalName(identifier: String): Boolean
  def isSystemColumnName(identifier: String): Boolean
  def magicDeleteKey: String
  def systemSchema: Map[String, String]
  def systemIdColumnName: String
}

class Mutator[CT, CV](common: MutatorCommon[CT, CV]) {
  import Mutator._
  import common._

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
        case other =>
          throw new InvalidCommandFieldValue(0, JString(other), "c")
      }
      new CommandStream(streamType, dataset, user, fatalRowErrors, remainingCommands.buffered)
    }

  def apply(u: Universe[CT, CV] with DatasetMutatorProvider, commandStream: Iterator[JValue]) {
    if(commandStream.isEmpty) throw EmptyCommandStream()
    val commands = createCommandStream(commandStream.next(), commandStream)
    def user = commands.user

    commands.streamType match {
      case NormalMutation =>
        for(ctx <- u.datasetMutator.openDataset(user)(commands.datasetName)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
      case CreateDatasetMutation =>
        for(mutator <- u.datasetMutator.createDataset(user)(commands.datasetName, "t")) { // TODO: Already exists
          for((col, typ) <- systemSchema) {
            val ci = mutator.addColumn(col, typ, physicalColumnBaseBase(col, systemColumn = true))
            if(col == systemIdColumnName) {
              mutator.makeSystemPrimaryKey(ci)
            }
          }
          carryOutCommands(mutator, commands)
        }
      case CreateWorkingCopyMutation(copyData) =>
        for(ctx <- u.datasetMutator.createCopy(user)(commands.datasetName, copyData = copyData)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
      case PublishWorkingCopyMutation(keepingSnapshotCount) =>
        for(ctx <- u.datasetMutator.publishCopy(user)(commands.datasetName)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
      case DropWorkingCopyMutation =>
        for(ctx <- u.datasetMutator.dropCopy(user)(commands.datasetName)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
    }
  }

  def carryOutCommands(mutator: DatasetMutator[CV]#MutationContext, commands: CommandStream): Seq[Report[CV]] = {
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

  def carryOutCommand(mutator: DatasetMutator[CV]#MutationContext, commands: CommandStream, cmd: Command): Option[Report[CV]] = {
    import mutator._
    cmd match {
      case AddColumn(name, typ) =>
        if(!isLegalLogicalName(name)) ??? // TODO: proper error
        if(schemaByLogicalName.contains(name)) ??? // TODO: proper error
        addColumn(name, typ, physicalColumnBaseBase(name)) // TODO: I should really be giving this a CT instance instead of "typ"
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

  def processRowData(rows: BufferedIterator[JValue], fatalRowErrors: Boolean, mutator: DatasetMutator[CV]#MutationContext): Report[CV] = {
    import mutator._
    val result = mutator.upsert(
      new Iterator[Either[CV, Row[CV]]] {
        val plan = new RowDecodePlan(schema, repFor, magicDeleteKey)
        def hasNext = rows.hasNext && JNull != rows.head
        def next() = {
          if(!hasNext) throw new NoSuchElementException
          plan(rows.next)
        }
      })
    if(rows.hasNext && JNull == rows.head) rows.next()
    if(fatalRowErrors && result.errors.nonEmpty) ??? // TODO: Error
    result
  }
}
