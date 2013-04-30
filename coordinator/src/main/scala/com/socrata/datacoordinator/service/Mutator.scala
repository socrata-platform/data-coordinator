package com.socrata.datacoordinator
package service

import com.rojoma.json.ast._
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.truth.{DatasetIdInUseByWriterException, DatasetMutator}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetCopyContext, LifecycleStage, ColumnInfo}
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.loader._
import scala.collection.immutable.VectorBuilder
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.datacoordinator.util.{BuiltUpIterator, Counter}
import com.ibm.icu.util.ULocale
import com.rojoma.json.io._
import com.socrata.datacoordinator.id.DatasetId

object Mutator {
  sealed abstract class StreamType {
    def index: Long
  }

  case class NormalMutation(index: Long, datasetId: DatasetId, schemaHash: Option[String]) extends StreamType
  case class CreateDatasetMutation(index: Long, localeName: String) extends StreamType
  case class CreateWorkingCopyMutation(index: Long, datasetId: DatasetId, copyData: Boolean, schemaHash: Option[String]) extends StreamType
  case class PublishWorkingCopyMutation(index: Long, datasetId: DatasetId, keepingSnapshotCount: Option[Int], schemaHash: Option[String]) extends StreamType
  case class DropWorkingCopyMutation(index: Long, datasetId: DatasetId, schemaHash: Option[String]) extends StreamType

  sealed abstract class MutationException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause) {
    def index: Long
  }

  sealed abstract class InvalidCommandStreamException(msg: String = null, cause: Throwable = null) extends MutationException(msg, cause)
  case class EmptyCommandStream()(val index: Long) extends InvalidCommandStreamException
  case class CommandIsNotAnObject(value: JValue)(val index: Long) extends InvalidCommandStreamException
  case class MissingCommandField(obj: JObject, field: String)(val index: Long) extends InvalidCommandStreamException
  case class InvalidCommandFieldValue(obj: JObject, field: String, value: JValue)(val index: Long) extends InvalidCommandStreamException
  case class MismatchedSchemaHash(name: DatasetId, schema: Schema)(val index: Long) extends InvalidCommandStreamException

  case class NoSuchDataset(name: DatasetId)(val index: Long) extends MutationException
  case class CannotAcquireDatasetWriteLock(name: DatasetId)(val index: Long) extends MutationException
  case class IncorrectLifecycleStage(name: DatasetId, currentLifecycleStage: LifecycleStage, expected: Set[LifecycleStage])(val index: Long) extends MutationException
  case class InitialCopyDrop(name: DatasetId)(val index: Long) extends MutationException
  case class IllegalColumnName(name: ColumnName)(val index: Long) extends MutationException
  case class NoSuchColumn(dataset: DatasetId, name: ColumnName)(val index: Long) extends MutationException
  case class NoSuchType(name: TypeName)(val index: Long) extends MutationException
  case class ColumnAlreadyExists(dataset: DatasetId, name: ColumnName)(val index: Long) extends MutationException
  case class PrimaryKeyAlreadyExists(dataset: DatasetId, name: ColumnName, existingName: ColumnName)(val index: Long) extends MutationException
  case class InvalidTypeForPrimaryKey(dataset: DatasetId, name: ColumnName, typ: TypeName)(val index: Long) extends MutationException
  case class NullsInColumn(dataset: DatasetId, name: ColumnName)(val index: Long) extends MutationException
  case class NotPrimaryKey(dataset: DatasetId, name: ColumnName)(val index: Long) extends MutationException
  case class DuplicateValuesInColumn(dataset: DatasetId, name: ColumnName)(val index: Long) extends MutationException
  case class InvalidSystemColumnOperation(dataset: DatasetId, name: ColumnName, op: String)(val index: Long) extends MutationException

  sealed abstract class RowDataException extends MutationException {
    def subindex: Int
  }
  case class InvalidUpsertCommand(value: JValue)(val index: Long, val subindex: Int) extends RowDataException
  case class InvalidValue(column: ColumnName, typ: TypeName, value: JValue)(val index: Long, val subindex: Int) extends RowDataException
  case class UpsertError(dataset: DatasetId, failure: Failure[JValue])(val index: Long) extends MutationException

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
    def originalObject: JObject
    def fields: scala.collection.Map[String, JValue] = originalObject.fields
    def get[T: JsonCodec](field: String): T
    def getOption[T: JsonCodec](field: String): Option[T]
    def getWithDefault[T: JsonCodec](field: String, default: T): T
  }

  def withObjectFields[A](index: Long, value: JValue)(f: Accessor => A): A = value match {
    case obj: JObject =>
      f(new Accessor {
        val originalObject = obj
        def get[T : JsonCodec](field: String) = {
          val json = fields.getOrElse(field, throw MissingCommandField(originalObject, field)(index))
          JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(obj, field, json)(index))
        }
        def getWithDefault[T : JsonCodec](field: String, default: T) = {
          fields.get(field) match {
            case Some(json) =>
              JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(obj, field, json)(index))
            case None =>
              default
          }
        }
        def getOption[T : JsonCodec](field: String) =
          fields.get(field).map { json =>
            JsonCodec[T].decode(json).getOrElse(throw InvalidCommandFieldValue(obj, field, json)(index))
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
  case class RowData(index: Long, truncate: Boolean, mergeReplace: MergeReplace, fatalRowErrors: Boolean) extends Command

  val AddColumnOp = "add column"
  val DropColumnOp = "drop column"
  val RenameColumnOp = "rename column"
  val SetRowIdOp = "set row id"
  val DropRowIdOp = "drop row id"
  val RowDataOp = "row data"

  class CommandStream(val streamType: StreamType, val user: String, val rawCommandStream: BufferedIterator[JValue]) {
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
          val fatalRowErrors = getWithDefault("fatal_row_errors", true)
          RowData(index, truncate, mergeReplace, fatalRowErrors)
        case other =>
          throw InvalidCommandFieldValue(originalObject, "c", JString(other))(index)
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
  def jsonReps(di: DatasetInfo): ColumnInfo[CT] => JsonColumnRep[CT, CV]
  def schemaFinder: SchemaFinder[CT, CV]
}

class Mutator[CT, CV](common: MutatorCommon[CT, CV]) {
  import Mutator._
  import common._

  def createCreateStream(index: Long, value: JValue, remainingCommands: Iterator[JValue]) =
    withObjectFields(index, value) { accessor =>
      import accessor._
      val streamType = get[String]("c") match {
        case "create" =>
          val locale = ULocale.createCanonical(getWithDefault("locale", "en_US"))
          CreateDatasetMutation(index, locale.getName)
        case other =>
          throw InvalidCommandFieldValue(originalObject, "c", JString(other))(index)
      }
      val user = get[String]("user")
      new CommandStream(streamType, user, remainingCommands.buffered)
    }

  def createCommandStream(index: Long, value: JValue, datasetId: DatasetId, remainingCommands: Iterator[JValue]) =
    withObjectFields(index, value) { accessor =>
      import accessor._
      val command = get[String]("c")
      val streamType = command match {
        case "copy" =>
          val copyData = get[Boolean]("copy_data")
          val schemaHash = getOption[String]("schema")
          CreateWorkingCopyMutation(index, datasetId, copyData, schemaHash)
        case "publish" =>
          val snapshotLimit = getOption[Int]("snapshot_limit")
          val schemaHash = getOption[String]("schema")
          PublishWorkingCopyMutation(index, datasetId, snapshotLimit, schemaHash)
        case "drop" =>
          val schemaHash = getOption[String]("schema")
          DropWorkingCopyMutation(index, datasetId, schemaHash)
        case "normal" =>
          val schemaHash = getOption[String]("schema")
          NormalMutation(index, datasetId, schemaHash)
        case other =>
          throw InvalidCommandFieldValue(originalObject, "c", JString(other))(index)
      }
      val user = get[String]("user")
      new CommandStream(streamType, user, remainingCommands.buffered)
    }

  def mapToEvents[T](m: collection.Map[Int,T])(implicit codec: JsonCodec[T]): Iterator[JsonEvent] = {
    def elemToStream(kv: (Int, T)) =
      new BuiltUpIterator(Iterator.single(FieldEvent(kv._1.toString)), JValueEventIterator(codec.encode(kv._2)))
    new BuiltUpIterator(
      Iterator.single(StartOfObjectEvent()),
      m.iterator.flatMap(elemToStream),
      Iterator.single(EndOfObjectEvent()))
  }

  def toEventStream(inserted: collection.Map[Int, JValue],
                    updated: collection.Map[Int, JValue],
                    deleted: collection.Map[Int, JValue],
                    elided: collection.Map[Int, (JValue, Int)],
                    errors: collection.Map[Int, Failure[JValue]]) = {
    new BuiltUpIterator(
      Iterator(StartOfObjectEvent(), FieldEvent("inserted")),
      mapToEvents(inserted),
      Iterator.single(FieldEvent("updated")),
      mapToEvents(updated),
      Iterator.single(FieldEvent("deleted")),
      mapToEvents(deleted),
      Iterator.single(FieldEvent("elided")),
      mapToEvents(elided),
      Iterator.single(FieldEvent("errors")),
      mapToEvents(errors),
      Iterator.single(EndOfObjectEvent()))
  }

  def createScript(u: Universe[CT, CV] with DatasetMutatorProvider, commandStream: Iterator[JValue]): (DatasetId, Iterator[JsonEvent]) = {
    if(commandStream.isEmpty) throw EmptyCommandStream()(0L)
    val commands = createCreateStream(0L, commandStream.next(), commandStream)
    runScript(u, commands)
  }

  def updateScript(u: Universe[CT, CV] with DatasetMutatorProvider, datasetId: DatasetId, commandStream: Iterator[JValue]): Iterator[JsonEvent] = {
    if(commandStream.isEmpty) throw EmptyCommandStream()(0L)
    val commands = createCommandStream(0L, commandStream.next(), datasetId, commandStream)
    runScript(u, commands)._2
  }

  private def runScript(u: Universe[CT, CV] with DatasetMutatorProvider, commands: CommandStream): (DatasetId, Iterator[JsonEvent]) = {
    def user = commands.user

    def doProcess(ctx: DatasetMutator[CT, CV]#MutationContext): (DatasetId, Iterator[JsonEvent]) = {
      val jsonRepFor = jsonReps(ctx.copyInfo.datasetInfo)
      val processor = new Processor(jsonRepFor)
      val events = processor.carryOutCommands(ctx, commands).map { r =>
        val pk = ctx.schema.values.find(_.isUserPrimaryKey).orElse(ctx.schema.values.find(_.isSystemPrimaryKey)).getOrElse {
          sys.error("No primary key on this dataset?")
        }
        val repify = jsonRepFor(pk).toJValue _
        val interim = new Report[JValue] {
          def inserted: collection.Map[Int, JValue] = r.inserted.mapValues(repify)

          /** Map from job number to the identifier of the row that was updated. */
          def updated: collection.Map[Int, JValue] = r.updated.mapValues(repify)

          /** Map from job number to the identifier of the row that was deleted. */
          def deleted: collection.Map[Int, JValue] = r.deleted.mapValues(repify)

          /** Map from job number to the identifier of the row that was merged with another job, and the job it was merged with. */
          def elided: collection.Map[Int, (JValue, Int)] = r.elided.mapValues { case (v, i) => (repify(v), i) }

          /** Map from job number to a value explaining the cause of the problem.. */
          def errors: collection.Map[Int, Failure[JValue]] = r.errors.mapValues(_.map(repify))
        }
        toEventStream(interim.inserted, interim.updated, interim.deleted, interim.elided, interim.errors)
      }
      (ctx.copyInfo.datasetInfo.systemId,
        new BuiltUpIterator(
          Iterator.single(StartOfArrayEvent()),
          new BuiltUpIterator(events: _*),
          Iterator.single(EndOfArrayEvent())))
    }

    def checkHash(index: Long, schemaHash: Option[String], ctx: DatasetCopyContext[CT]) {
      for(givenSchemaHash <- schemaHash) {
        val realSchemaHash = schemaFinder.schemaHash(ctx.schema)
        if(givenSchemaHash != realSchemaHash) {
          throw MismatchedSchemaHash(ctx.datasetInfo.systemId, schemaFinder.getSchema(ctx.schema))(index)
        }
      }
    }

    def process(index: Long, datasetId: DatasetId, mutator: DatasetMutator[CT, CV])(maybeCtx: mutator.CopyContext) = maybeCtx match {
      case mutator.CopyOperationComplete(ctx) =>
        doProcess(ctx)
      case mutator.IncorrectLifecycleStage(currentStage, expectedStages) =>
        throw IncorrectLifecycleStage(datasetId, currentStage, expectedStages)(index)
      case mutator.DatasetDidNotExist =>
        throw NoSuchDataset(datasetId)(index)
    }

    try {
      val mutator = u.datasetMutator
      commands.streamType match {
        case NormalMutation(idx, datasetId, schemaHash) =>
          for(ctxOpt <- mutator.openDataset(user)(datasetId, checkHash(idx, schemaHash, _))) yield {
            val ctx = ctxOpt.getOrElse { throw NoSuchDataset(datasetId)(idx) }
            doProcess(ctx)
          }
        case CreateDatasetMutation(idx, localeName) =>
          for(ctx <- u.datasetMutator.createDataset(user)("t", localeName)) yield {
            for((col, typ) <- systemSchema) {
              val ci = ctx.addColumn(col, typ, physicalColumnBaseBase(col, systemColumn = true))
              if(col == systemIdColumnName) {
                ctx.makeSystemPrimaryKey(ci)
              }
            }
            doProcess(ctx)
          }
        case CreateWorkingCopyMutation(idx, datasetId, copyData, schemaHash) =>
          mutator.createCopy(user)(datasetId, copyData = copyData, checkHash(idx, schemaHash, _)).map(process(idx, datasetId, mutator))
        case PublishWorkingCopyMutation(idx, datasetId, keepingSnapshotCount, schemaHash) =>
          mutator.publishCopy(user)(datasetId, keepingSnapshotCount, checkHash(idx, schemaHash, _)).map(process(idx, datasetId, mutator))
        case DropWorkingCopyMutation(idx, datasetId, schemaHash) =>
          mutator.dropCopy(user)(datasetId, checkHash(idx, schemaHash, _)).map {
            case cc: mutator.CopyContext =>
              process(idx, datasetId, mutator)(cc)
            case mutator.InitialWorkingCopy =>
              throw InitialCopyDrop(datasetId)(idx)
          }
      }
    } catch {
      case e: DatasetIdInUseByWriterException =>
        throw CannotAcquireDatasetWriteLock(e.datasetId)(commands.streamType.index)
    }
  }

  class Processor(jsonRepFor: ColumnInfo[CT] => JsonColumnRep[CT, CV]) {
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
      def datasetId = mutator.copyInfo.datasetInfo.systemId
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
              throw ColumnAlreadyExists(datasetId, name)(idx)
          }
        case DropColumn(idx, name) =>
          mutator.columnInfo(name) match {
            case Some(colInfo) =>
              if(isSystemColumnName(name)) throw InvalidSystemColumnOperation(datasetId, name, DropColumnOp)(idx)
              mutator.dropColumn(colInfo)
            case None =>
              throw NoSuchColumn(datasetId, name)(idx)
          }
          None
        case RenameColumn(idx, from, to) =>
          mutator.columnInfo(from) match {
            case Some(colInfo) =>
              if(isSystemColumnName(from)) throw InvalidSystemColumnOperation(datasetId, from, RenameColumnOp)(idx)
              if(!isLegalLogicalName(to)) throw IllegalColumnName(to)(idx)
              if(mutator.columnInfo(to).isDefined) throw ColumnAlreadyExists(datasetId, to)(idx)
              mutator.renameColumn(colInfo, to)
            case None =>
              throw NoSuchColumn(datasetId, from)(idx)
          }
          None
        case SetRowId(idx, name) =>
          mutator.columnInfo(name) match {
            case Some(colInfo) =>
              for(pkCol <- mutator.schema.values.find(_.isUserPrimaryKey))
                throw PrimaryKeyAlreadyExists(datasetId, name, pkCol.logicalName)(idx)
              if(isSystemColumnName(name)) throw InvalidSystemColumnOperation(datasetId, name, SetRowIdOp)(idx)
              try {
                mutator.makeUserPrimaryKey(colInfo)
              } catch {
                case e: mutator.PrimaryKeyCreationException => e match {
                  case mutator.UnPKableColumnException(_, _) =>
                    throw InvalidTypeForPrimaryKey(datasetId, colInfo.logicalName, typeNameFor(colInfo.typ))(idx)
                  case mutator.NullCellsException(c) =>
                    throw NullsInColumn(datasetId, colInfo.logicalName)(idx)
                  case mutator.DuplicateCellsException(_) =>
                    throw DuplicateValuesInColumn(datasetId, colInfo.logicalName)(idx)
                }
              }
            case None =>
              throw NoSuchColumn(datasetId, name)(idx)
          }
          None
        case DropRowId(idx, name) =>
          mutator.columnInfo(name) match {
            case Some(colInfo) =>
              if(!colInfo.isUserPrimaryKey) throw NotPrimaryKey(datasetId, name)(idx)
              mutator.unmakeUserPrimaryKey(colInfo)
            case None =>
              throw NoSuchColumn(datasetId, name)(idx)
          }
          None
        case RowData(idx, truncate, mergeReplace, fatalRowErrors) =>
          if(mergeReplace == Replace) ??? // TODO: implement this
          if(truncate) mutator.truncate()
          Some(processRowData(idx, commands.rawCommandStream, fatalRowErrors, mutator))
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
          val pk = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
            sys.error("No primary key on this dataset?")
          }
          val trueError = result.errors.minBy(_._1)._2.map(jsonRepFor(pk).toJValue)
          throw UpsertError(mutator.copyInfo.datasetInfo.systemId, trueError)(idx)
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
