package com.socrata.datacoordinator.service.mutator

import com.socrata.datacoordinator.id.{DatasetId, UserColumnId}
import com.socrata.soql.environment.TypeName
import com.rojoma.json.ast.{JString, JObject, JValue}

import MutatorLib._
import com.socrata.datacoordinator.truth.universe.{DatasetMapReaderProvider, SchemaFinderProvider, DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.truth.{DatabaseInReadOnlyMode, DatasetIdInUseByWriterException, TypeContext, DatasetMutator}
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, LifecycleStage, DatasetInfo}
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import scala.collection.mutable
import org.joda.time.DateTime
import com.ibm.icu.util.ULocale
import com.socrata.datacoordinator.util.collection.UserColumnIdMap

trait DDLMutatorCommon[CT, CV] {
  def allowDdlOnPublishedCopies: Boolean
  def genUserColumnId(): UserColumnId
  def isSystemColumnId(identifier: UserColumnId): Boolean
  def typeContext: TypeContext[CT, CV]
  def physicalColumnBaseBase(nameHint: String, systemColumn: Boolean = false): String
  def systemSchema: UserColumnIdMap[CT]
  def systemIdColumnId: UserColumnId
  def versionColumnId: UserColumnId
}

class DDLMutator[CT,CV](common: DDLMutatorCommon[CT, CV]) {
  import DDLException._
  import MutationScriptHeaderException._
  import common._

  val AddColumnOp = "add column"
  val DropColumnOp = "drop column"
  val RenameColumnOp = "rename column"
  val SetRowIdOp = "set row id"
  val DropRowIdOp = "drop row id"

  sealed abstract class Command {
    def index: Long
  }
  case class AddColumn(index: Long, nameHint: String, typ: TypeName, label: Option[String]) extends Command
  case class DropColumn(index: Long, idOrLabel: Either[UserColumnId, ColumnIdLabel]) extends Command
  case class SetRowId(index: Long, idOrLabel: Either[UserColumnId, ColumnIdLabel]) extends Command
  case class DropRowId(index: Long, idOrLabel: Either[UserColumnId, ColumnIdLabel]) extends Command

  case class ColumnIdLabel(label: String)
  implicit val ColumnIdLabelCodec = AutomaticJsonCodecBuilder[ColumnIdLabel]

  class CommandStream(val rawCommandStream: Iterator[JValue]) {
    private var idx = 1L
    private def nextIdx() = {
      val res = idx
      idx += 1
      res
    }

    private def decodeCommand(index: Long, json: JValue): Command = {
      val onError = new OnError {
        def missingField(obj: JObject, field: String) =
          throw MissingCommandField(obj, field, index)
        def invalidValue(obj: JObject, field: String, value: JValue) =
          throw InvalidCommandFieldValue(obj, field, json, index)
        def notAnObject(value: JValue) =
          throw new CommandIsNotAnObject(value, index)
      }
      withObjectFields(json, onError) { accessor =>
        import accessor._
        get[String]("command") match {
          case AddColumnOp =>
            val typ = get[String]("type")
            val nameHint = getWithStrictDefault("hint", typ)
            val label = getOption[String]("label")
            AddColumn(index, nameHint, TypeName(typ), label)
          case DropColumnOp =>
            val column = get[Either[UserColumnId, ColumnIdLabel]]("column")
            DropColumn(index, column)
          case SetRowIdOp =>
            val column = get[Either[UserColumnId, ColumnIdLabel]]("column")
            SetRowId(index, column)
          case DropRowIdOp =>
            val column = get[Either[UserColumnId, ColumnIdLabel]]("column")
            DropRowId(index, column)
          case other =>
            onError.invalidValue(originalObject, "command", JString(other))
        }
      }
    }

    def nextCommand() =
      if(rawCommandStream.hasNext) Some(decodeCommand(nextIdx(), rawCommandStream.next()))
      else None
  }

  case class CreateHeader(user: String, localeName: String)
  case class DDLHeader(user: String, schemaHash: Option[String])

  sealed abstract class CopyCommand {
    val user: String
    val schemaHash: Option[String]
  }
  case class MakeWorkingCopy(user: String, schemaHash: Option[String], copyData: Boolean) extends CopyCommand
  case class DropWorkingCopy(user: String, schemaHash: Option[String]) extends CopyCommand
  case class PublishWorkingCopy(user: String, schemaHash: Option[String]) extends CopyCommand

  private def getDDLHeader(value: JValue): DDLHeader = {
    withObjectFields(value, HeaderError) { accessor =>
      import accessor._
      val user = get[String]("user")
      val schemaHash = getOption[String]("schemaHash")
      DDLHeader(user, schemaHash)
    }
  }

  private def getCreateHeader(value: JValue): CreateHeader = {
    withObjectFields(value, HeaderError) { accessor =>
      import accessor._
      val user = get[String]("user")
      val rawLocale = getWithStrictDefault("locale", "en_US")
      val locale = ULocale.createCanonical(rawLocale)
      if(locale.getName != "en_US") throw InvalidLocale(rawLocale) // for now, we only allow en_US
      CreateHeader(user, locale.getName)
    }
  }

  private def getCopyCommand(value: JValue): CopyCommand = {
    withObjectFields(value, HeaderError) { accessor =>
      import accessor._
      val user = get[String]("user")
      val schemaHash = getOption[String]("schemaHash")
      get[String]("command") match {
        case "copy" =>
          val copyData = get[Boolean]("copy_data")
          MakeWorkingCopy(user, schemaHash, copyData)
        case "publish" =>
          PublishWorkingCopy(user, schemaHash)
        case "drop" =>
          DropWorkingCopy(user, schemaHash)
        case other =>
          HeaderError.invalidValue(originalObject, "command", JString(other))
      }
    }
  }

  def createScript(u: Universe[CT, CV] with DatasetMutatorProvider with DatasetMapReaderProvider, commandStream: Iterator[JValue]): (DatasetId, Long, DateTime, Seq[MutationScriptCommandResult]) = {
    translatingCannotWriteExceptions {
      if(commandStream.isEmpty) throw EmptyCommandStream()
      val createHeader = getCreateHeader(commandStream.next())
      val (datasetId, mutationResults) =
        for(ctx <- u.datasetMutator.createDataset(createHeader.user)(createHeader.localeName)) yield {
          val cis = ctx.addColumns(systemSchema.toSeq.map { case (col, typ) =>
            ctx.ColumnToAdd(col, typ, physicalColumnBaseBase(col.underlying, systemColumn = true))
          })
          for(ci <- cis) {
            val ci2 =
              if(ci.userColumnId == systemIdColumnId) ctx.makeSystemPrimaryKey(ci)
              else ci
            if(ci2.userColumnId == versionColumnId) ctx.makeVersion(ci2)
          }
          (ctx.copyInfo.datasetInfo.systemId, runDDLScript(ctx, commandStream))
        }
      val copyInfo = u.datasetMapReader.latest(u.datasetMapReader.datasetInfo(datasetId).get)
      (datasetId, copyInfo.dataVersion, copyInfo.lastModified, mutationResults)
    }
  }

  def copyOp(u: Universe[CT, CV] with DatasetMutatorProvider with SchemaFinderProvider with DatasetMapReaderProvider, datasetId: DatasetId, command: JValue): (Long, DateTime) = {
    translatingCannotWriteExceptions {
      val mutator = u.datasetMutator

      val opCtx = getCopyCommand(command) match {
        case MakeWorkingCopy(user, schema, copyData) =>
          mutator.createCopy(user)(datasetId, copyData, checkHash(u, schema, _))
        case DropWorkingCopy(user, schema) =>
          mutator.dropCopy(user)(datasetId, checkHash(u, schema, _))
        case PublishWorkingCopy(user, schema) =>
          mutator.publishCopy(user)(datasetId, Some(0), checkHash(u, schema, _))
      }
      opCtx.flatMap {
        case mutator.InitialWorkingCopy => throw InitialCopyDrop(datasetId)
        case mutator.DatasetDidNotExist() => throw NoSuchDataset(datasetId)
        case mutator.CopyOperationComplete(_) => // ok
        case mutator.IncorrectLifecycleStage(current, expected) => throw IncorrectLifecycleStage(datasetId, current, expected)
      }
      val copyInfo = u.datasetMapReader.latest(u.datasetMapReader.datasetInfo(datasetId).get)
      (copyInfo.dataVersion, copyInfo.lastModified)
    }
  }

  def updateScript(u: Universe[CT, CV] with DatasetMutatorProvider with SchemaFinderProvider with DatasetMapReaderProvider, datasetId: DatasetId, commandStream: Iterator[JValue]): (Long, DateTime, Seq[MutationScriptCommandResult]) = {
    translatingCannotWriteExceptions {
      if(commandStream.isEmpty) throw EmptyCommandStream()
      val ddlHeader = getDDLHeader(commandStream.next())
      val mutationResults = for(ctxOpt <- u.datasetMutator.openDataset(ddlHeader.user)(datasetId, checkHash(u, ddlHeader.schemaHash, _))) yield {
        val ctx = ctxOpt.getOrElse { throw NoSuchDataset(datasetId) }
        runDDLScript(ctx, commandStream)
      }
      val copyInfo = u.datasetMapReader.latest(u.datasetMapReader.datasetInfo(datasetId).get)
      (copyInfo.dataVersion, copyInfo.lastModified, mutationResults)
    }
  }

  private def runDDLScript(mutator: DatasetMutator[CT, CV]#MutationContext, rawCommands: Iterator[JValue]): Seq[MutationScriptCommandResult] = {
    val commands = new CommandStream(rawCommands)
    if(!allowDdlOnPublishedCopies && mutator.copyInfo.lifecycleStage != LifecycleStage.Unpublished)
      throw IncorrectLifecycleStage(mutator.copyInfo.datasetInfo.systemId, mutator.copyInfo.lifecycleStage, Set(LifecycleStage.Unpublished))
    val processor = new Processor(mutator)
    processor.carryOutCommands(commands)
  }

  private class Processor(mutator: DatasetMutator[CT, CV]#MutationContext) {
    val datasetId = mutator.copyInfo.datasetInfo.systemId

    def typeNameFor(typ: CT): TypeName =
      typeContext.typeNamespace.userTypeForType(typ)

    def typeForNameOpt(name: TypeName): Option[CT] =
      typeContext.typeNamespace.typeForUserType(name)

    def carryOutCommands(commands: CommandStream): Seq[MutationScriptCommandResult] = {
      val reports = new VectorBuilder[MutationScriptCommandResult]
      def loop() {
        commands.nextCommand() match {
          case Some(cmd) => reports ++= carryOutCommand(commands, cmd); loop()
          case None => reports ++= flushPendingCommands()
        }
      }
      loop()
      reports.result()
    }

    private val pendingAdds = new ListBuffer[mutator.ColumnToAdd]
    private val pendingDrops = new ListBuffer[UserColumnId]
    private val labels = new mutable.HashMap[String, UserColumnId]

    def idFor(idx: Long, idOrLabel: Either[UserColumnId, ColumnIdLabel]): UserColumnId =
      idOrLabel match {
        case Left(id) => id
        case Right(label) => labels.getOrElse(label.label, throw NoSuchColumnLabel(label.label, idx))
      }

    def isExistingColumn(cid: UserColumnId) =
      (mutator.schema.iterator.map(_._2.userColumnId).exists(_ == cid) || pendingAdds.exists(_.userColumnId == cid)) && !pendingDrops.exists(_ == cid)

    def createId(): UserColumnId = {
      var id = genUserColumnId()
      while(isExistingColumn(id)) id = genUserColumnId()
      id
    }

    def flushPendingCommands(): Seq[MutationScriptCommandResult] = {
      assert(pendingAdds.isEmpty || pendingDrops.isEmpty, "Have both pending adds and pending drops?")

      val addResults = if(pendingAdds.nonEmpty) {
        val cis = mutator.addColumns(pendingAdds)
        val res = cis.toVector.map { ci =>
          MutationScriptCommandResult.ColumnCreated(ci.userColumnId, ci.typeNamespace.userTypeForType(ci.typ))
        }
        pendingAdds.clear()
        res
      } else Vector.empty

      val dropResults = if(pendingDrops.nonEmpty) {
        mutator.dropColumns(pendingDrops.map { cid => mutator.columnInfo(cid).getOrElse { sys.error("I verified column " + cid + " existed before adding it to the list for dropping?") } })
        val res = Vector.fill(pendingDrops.size) { MutationScriptCommandResult.Uninteresting }
        pendingDrops.clear()
        res
      } else {
        Vector.empty
      }

      addResults ++ dropResults
    }

    def carryOutCommand(commands: CommandStream, cmd: Command): Seq[MutationScriptCommandResult] = {
      val pendingResults =
        if(!cmd.isInstanceOf[AddColumn] && pendingAdds.nonEmpty) flushPendingCommands()
        else if(!cmd.isInstanceOf[DropColumn] && pendingDrops.nonEmpty) flushPendingCommands()
        else Vector.empty

      val newResults = cmd match {
        case AddColumn(idx, nameHint, typName, label) =>
          val typ = typeForNameOpt(typName).getOrElse {
            throw NoSuchType(typName, idx)
          }
          val id = createId()
          pendingAdds += mutator.ColumnToAdd(id, typ, physicalColumnBaseBase(nameHint))
          label.foreach { l =>
            labels(l) = id
          }
          Nil
        case DropColumn(idx, idOrLabel) =>
          val id = idFor(idx, idOrLabel)
          if(!isExistingColumn(id)) throw NoSuchColumn(datasetId, id, idx)
          if(isSystemColumnId(id)) throw InvalidSystemColumnOperation(datasetId, id, DropColumnOp, idx)
          if(mutator.columnInfo(id).get.isUserPrimaryKey) throw DeleteRowIdentifierNotAllowed(datasetId, id, idx)
          pendingDrops += id
          Nil
        case SetRowId(idx, idOrLabel) =>
          val id = idFor(idx, idOrLabel)
          mutator.columnInfo(id) match {
            case Some(colInfo) =>
              for(pkCol <- mutator.schema.values.find(_.isUserPrimaryKey))
                throw PrimaryKeyAlreadyExists(datasetId, id, pkCol.userColumnId, idx)
              if(isSystemColumnId(id)) throw InvalidSystemColumnOperation(datasetId, id, SetRowIdOp, idx)
              try {
                mutator.makeUserPrimaryKey(colInfo)
              } catch {
                case e: mutator.PrimaryKeyCreationException => e match {
                  case mutator.UnPKableColumnException(_, _) =>
                    throw InvalidTypeForPrimaryKey(datasetId, colInfo.userColumnId, typeNameFor(colInfo.typ), idx)
                  case mutator.NullCellsException(c) =>
                    throw NullsInColumn(datasetId, colInfo.userColumnId, idx)
                  case mutator.DuplicateCellsException(_) =>
                    throw DuplicateValuesInColumn(datasetId, colInfo.userColumnId, idx)
                }
              }
            case None =>
              throw NoSuchColumn(datasetId, id, idx)
          }
          Seq(MutationScriptCommandResult.Uninteresting)
        case DropRowId(idx, idOrLabel) =>
          val id = idFor(idx, idOrLabel)
          mutator.columnInfo(id) match {
            case Some(colInfo) =>
              if(!colInfo.isUserPrimaryKey) throw NotPrimaryKey(datasetId, id, idx)
              mutator.unmakeUserPrimaryKey(colInfo)
            case None =>
              throw NoSuchColumn(datasetId, id, idx)
          }
          Seq(MutationScriptCommandResult.Uninteresting)
      }

      pendingResults ++ newResults
    }
  }
}
