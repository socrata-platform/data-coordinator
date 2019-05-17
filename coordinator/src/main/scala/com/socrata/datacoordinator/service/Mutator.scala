package com.socrata.datacoordinator
package service

import com.ibm.icu.util.ULocale
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.io._
import com.rojoma.json.v3.util.{Strategy, JsonKeyStrategy, AutomaticJsonDecodeBuilder}
import com.socrata.datacoordinator.common.MutatorCommon
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.DatabaseInReadOnlyMode
import com.socrata.datacoordinator.truth.{DatasetIdInUseByWriterException, DatasetMutator}
import com.socrata.datacoordinator.util.{IndexedTempFile, BuiltUpIterator, Counter}
import com.socrata.soql.environment.{TypeName, ColumnName}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.immutable.{NumericRange, VectorBuilder}
import scala.collection.mutable
import com.socrata.datacoordinator.util.jsoncodecs._

sealed trait MutationScriptCommandResult
object MutationScriptCommandResult {
  case class ColumnCreated(id: UserColumnId, typ: TypeName) extends MutationScriptCommandResult
  case object Uninteresting extends MutationScriptCommandResult
  case class RowData(results: NumericRange[Long]) extends MutationScriptCommandResult
}

object Mutator {
  sealed abstract class StreamType {
    def index: Long
  }

  case class NormalMutation(index: Long, datasetId: DatasetId, schemaHash: Option[String], expectedDataVersion: Option[Long]) extends StreamType
  case class CreateDatasetMutation(index: Long, localeName: String, resourceName: Option[String]) extends StreamType
  case class CreateWorkingCopyMutation(index: Long,
                                       datasetId: DatasetId,
                                       copyData: Boolean,
                                       schemaHash: Option[String],
                                       expectedDataVersion: Option[Long]) extends StreamType
  case class PublishWorkingCopyMutation(index: Long,
                                        datasetId: DatasetId,
                                        keepSnapshot: Boolean,
                                        schemaHash: Option[String],
                                        expectedDataVersion: Option[Long]) extends StreamType
  case class DropWorkingCopyMutation(index: Long,
                                     datasetId: DatasetId,
                                     schemaHash: Option[String],
                                     expectedDataVersion: Option[Long]) extends StreamType

  // MutationExceptions
  sealed abstract class MutationException(msg: String = null, cause: Throwable = null)
  extends Exception(msg, cause) {
    def index: Long
  }

  case class InvalidLocale(locale: String)(val index: Long) extends MutationException
  case class NoSuchDataset(name: DatasetId)(val index: Long) extends MutationException
  case class SecondaryStoresNotUpToDate(name: DatasetId, stores: Set[String])(val index: Long) extends MutationException
  case class NoSuchRollup(name: RollupName)(val index: Long) extends MutationException
  case class CannotAcquireDatasetWriteLock(name: DatasetId)(val index: Long) extends MutationException
  case class SystemInReadOnlyMode()(val index: Long) extends MutationException
  case class IncorrectLifecycleStage(name: DatasetId,
                                     currentLifecycleStage: LifecycleStage,
                                     expected: Set[LifecycleStage])(val index: Long) extends MutationException
  case class InitialCopyDrop(name: DatasetId)(val index: Long) extends MutationException
  case class OperationAfterDrop(name: DatasetId)(val index: Long) extends MutationException
  case class IllegalColumnId(id: UserColumnId)(val index: Long) extends MutationException
  case class NoSuchColumn(dataset: DatasetId, id: UserColumnId)(val index: Long) extends MutationException
  case class NoComputationStrategy(dataset: DatasetId, id: UserColumnId)(val index: Long) extends MutationException
  case class NoSuchType(name: TypeName)(val index: Long) extends MutationException
  case class ColumnAlreadyExists(dataset: DatasetId, id: UserColumnId)(val index: Long) extends MutationException
  case class FieldNameAlreadyExists(dataset: DatasetId, ColumnName: ColumnName)(val index: Long) extends MutationException
  case class PrimaryKeyAlreadyExists(dataset: DatasetId,
                                     id: UserColumnId,
                                     existingName: UserColumnId)(val index: Long) extends MutationException
  case class InvalidTypeForPrimaryKey(dataset: DatasetId,
                                      name: UserColumnId,
                                      typ: TypeName)(val index: Long) extends MutationException
  case class NullsInColumn(dataset: DatasetId, id: UserColumnId)(val index: Long) extends MutationException
  case class NotPrimaryKey(dataset: DatasetId, id: UserColumnId)(val index: Long) extends MutationException
  case class DuplicateValuesInColumn(dataset: DatasetId, id: UserColumnId) (val index: Long) extends MutationException
  case class InvalidSystemColumnOperation(dataset: DatasetId,
                                          id: UserColumnId,
                                          op: String)(val index: Long) extends MutationException
  case class UpsertError(dataset: DatasetId, failure: Failure[JValue], versionToJson: RowVersion => JValue)
                        (val index: Long) extends MutationException
  case class RowLacksPrimaryKey(dataset: DatasetId)(val index: Long) extends MutationException

  // InvalidCommandStreamExceptions
  sealed abstract class InvalidCommandStreamException(msg: String = null, cause: Throwable = null)
    extends MutationException(msg, cause)
  case class EmptyCommandStream()(val index: Long) extends InvalidCommandStreamException
  case class CommandIsNotAnObject(value: JValue)(val index: Long) extends InvalidCommandStreamException
  case class MissingCommandField(obj: JObject, field: String) (val index: Long) extends InvalidCommandStreamException
  case class InvalidCommandFieldValue(obj: JObject,
                                      field: String,
                                      value: JValue) (val index: Long) extends InvalidCommandStreamException
  case class MismatchedSchemaHash(name: DatasetId, schema: Schema)(val index: Long) extends InvalidCommandStreamException
  case class MismatchedDataVersion(name: DatasetId, dataVersion: Long)(val index: Long) extends InvalidCommandStreamException
  case class NoSuchColumnLabel(name: DatasetId, label: String)(val index: Long) extends InvalidCommandStreamException

  // RowDataExceptions
  sealed abstract class RowDataException extends MutationException {
    def subindex: Int
  }
  case class InvalidUpsertCommand(dataset: DatasetId, value: JValue)
                                 (val index: Long, val subindex: Int) extends RowDataException
  case class InvalidValue(dataset: DatasetId, column: UserColumnId, typ: TypeName, value: JValue)
                         (val index: Long, val subindex: Int) extends RowDataException
  case class UnknownColumnId(dataset: DatasetId, column: UserColumnId)
                            (val index: Long, val subindex: Int) extends RowDataException


  sealed abstract class MergeReplace
  object MergeReplace {
    implicit val jCodec = new JsonDecode[MergeReplace] with JsonEncode[MergeReplace] {
      val MergeS = JString("merge")
      val ReplaceS = JString("replace")
      def encode(x: MergeReplace): JValue = x match {
        case Merge => MergeS
        case Replace => ReplaceS
      }
      def decode(x: JValue): JsonDecode.DecodeResult[MergeReplace] = x match {
        case MergeS => Right(Merge)
        case ReplaceS => Right(Replace)
        case other => Left(DecodeError.InvalidValue(x))
      }
    }
  }
  case object Merge extends MergeReplace
  case object Replace extends MergeReplace

  trait Accessor {
    def originalObject: JObject
    def fields: scala.collection.Map[String, JValue] = originalObject.fields
    def get[T: JsonDecode](field: String): T
    def getOption[T: JsonDecode](field: String): Option[T]
    def getWithStrictDefault[T: JsonDecode](field: String, default: T): T
    def getWithLazyDefault[T: JsonDecode](field: String, default: => T): T
  }

  def withObjectFields[A](index: Long, value: JValue)(f: Accessor => A): A = value match {
    case obj: JObject =>
      f(new Accessor {
        val originalObject = obj
        def get[T : JsonDecode](field: String) = {
          val json = fields.getOrElse(field, throw MissingCommandField(originalObject, field)(index))
          JsonDecode[T].decode(json).right.toOption.getOrElse(throw InvalidCommandFieldValue(obj, field, json)(index))
        }
        def getWithStrictDefault[T : JsonDecode](field: String, default: T) = getWithLazyDefault(field, default)
        def getWithLazyDefault[T : JsonDecode](field: String, default: => T) = {
          fields.get(field) match {
            case Some(json) =>
              JsonDecode[T].decode(json).right.toOption.getOrElse(throw InvalidCommandFieldValue(obj, field, json)(index))
            case None =>
              default
          }
        }
        def getOption[T : JsonDecode](field: String) =
          fields.get(field).map { json =>
            JsonDecode[T].decode(json).right.toOption.getOrElse(throw InvalidCommandFieldValue(obj, field, json)(index))
          }
      })
    case other =>
      throw new CommandIsNotAnObject(other)(index)
  }

  case class ColumnLabel(label: String)
  implicit val columnLabelDecode = AutomaticJsonDecodeBuilder[ColumnLabel]

  type ColumnIdSpec = Either[ColumnLabel, UserColumnId]
  sealed abstract class Command {
    def index: Long
  }
  @JsonKeyStrategy(Strategy.Underscore)
  case class ComputationStrategySpec(strategyType: StrategyType, sourceColumnIds: Seq[ColumnIdSpec], parameters: JObject)
  implicit val compStratSpec = AutomaticJsonDecodeBuilder[ComputationStrategySpec]
  case class AddColumn(index: Long, id: Option[ColumnIdSpec], nameHint: String, ColumnName: Option[ColumnName], typ: TypeName, computationStrategy: Option[ComputationStrategySpec] = None) extends Command
  case class DropColumn(index: Long, id: ColumnIdSpec) extends Command
  case class UpdateFieldName(index: Long, id: ColumnIdSpec, fieldName: ColumnName) extends Command
  case class SetRowId(index: Long, id: ColumnIdSpec) extends Command
  case class DropRowId(index: Long, id: ColumnIdSpec) extends Command
  case class RowData(index: Long, truncate: Boolean, mergeReplace: MergeReplace,
                     nonfatalRowErrors: Set[Class[_ <: Failure[_]]],
                     updateOnly: Boolean,
                     bySystemId: Boolean) extends Command
  case class CreateOrUpdateRollup(index: Long, name: RollupName, soql: String) extends Command
  case class DropRollup(index: Long, name: RollupName) extends Command
  case class AddComputationStrategy(index: Long, id: ColumnIdSpec, computationStrategy: ComputationStrategySpec) extends Command
  case class DropComputationStrategy(index: Long, id: ColumnIdSpec) extends Command

  val AddColumnOp = "add column"
  val DropColumnOp = "drop column"
  val UpdateColumnNameOp = "update field name"
  val SetRowIdOp = "set row id"
  val DropRowIdOp = "drop row id"
  val RowDataOp = "row data"
  val CreateOrUpdateRollupOp = "create or update rollup"
  val DropRollupOp = "drop rollup"
  val AddComputationStrategyOp = "add computation strategy"
  val DropComputationStrategyOp = "drop computation strategy"
}

class Mutator[CT, CV](indexedTempFile: IndexedTempFile, common: MutatorCommon[CT, CV]) {
  import Mutator._
  import common._

  class CommandStream(val streamType: StreamType, val user: String,
                      val rawCommandStream: BufferedIterator[JValue]) {
    private var idx = 1L
    private def nextIdx() = {
      val res = idx
      idx += 1
      res
    }

    private def decodeCommand(index: Long, json: JValue): Command =
      withObjectFields(index, json) { accessor =>
        import accessor._

        get[String]("c") match {
          case AddColumnOp =>
            val id = getOption[ColumnIdSpec]("id")
            val typ = get[String]("type")
            val fieldName = getOption[ColumnName]("field_name")
            val nameHint = fieldName.fold(getWithStrictDefault[String]("hint", typ))(_.name)
            val computationStrategy = getOption[ComputationStrategySpec]("computation_strategy")
            AddColumn(index, id, nameHint, fieldName, TypeName(typ), computationStrategy)
          case AddComputationStrategyOp =>
            val column = get[ColumnIdSpec]("column")
            val computationStrategy = get[ComputationStrategySpec]("computation_strategy")
            AddComputationStrategy(index, column, computationStrategy)
          case DropComputationStrategyOp =>
            val column = get[ColumnIdSpec]("column")
            DropComputationStrategy(index, column)
          case DropColumnOp =>
            val column = get[ColumnIdSpec]("column")
            DropColumn(index, column)
          case UpdateColumnNameOp =>
            val column = get[ColumnIdSpec]("column")
            val fieldName = get[ColumnName]("field_name")
            UpdateFieldName(index, column, fieldName)
          case SetRowIdOp =>
            val column = get[ColumnIdSpec]("column")
            SetRowId(index, column)
          case DropRowIdOp =>
            val column = get[ColumnIdSpec]("column")
            DropRowId(index, column)
          case CreateOrUpdateRollupOp =>
            val name = get[RollupName]("name")
            val soql = get[String]("soql")
            CreateOrUpdateRollup(index, name, soql)
          case DropRollupOp =>
            val name = get[RollupName]("name")
            DropRollup(index, name)
          case RowDataOp =>
            val truncate = getWithStrictDefault("truncate", false)
            val mergeReplace = getWithStrictDefault[MergeReplace]("update", Merge)
            val nonFatalRowErrors = getOption[Seq[String]]("nonfatal_row_errors").getOrElse {
              if(getWithStrictDefault("fatal_row_errors", true)) Seq.empty[String] else Failure.allFailures.keys
            }
            val nonFatalRowErrorsClasses = nonFatalRowErrors.map { nfe =>
              Failure.allFailures.getOrElse(nfe, throw new InvalidCommandFieldValue(originalObject,
                                            "nonfatal_row_errors", JString(nfe))(index))
            }.toSet
            val updateOnly = getWithStrictDefault[Boolean]("update_only", false)
            val bySystemId = getWithStrictDefault[Boolean]("by_system_id", false)
            RowData(index, truncate, mergeReplace, nonFatalRowErrorsClasses, updateOnly = updateOnly, bySystemId = bySystemId)
          case other =>
            throw InvalidCommandFieldValue(originalObject, "c", JString(other))(index)
        }
      }

    def nextCommand() =
      if(rawCommandStream.hasNext) Some(decodeCommand(nextIdx(), rawCommandStream.next()))
      else None
  }

  def typeNameFor(typ: CT): TypeName =
    typeContext.typeNamespace.userTypeForType(typ)

  def nameForTypeOpt(name: TypeName): Option[CT] =
    typeContext.typeNamespace.typeForUserType(name)

  def createCreateStream(index: Long, value: JValue, remainingCommands: Iterator[JValue]) =
    withObjectFields(index, value) { accessor =>
      import accessor._
      val streamType = get[String]("c") match {
        case "create" =>
          val rawLocale = getWithStrictDefault("locale", "en_US")
          val locale = ULocale.createCanonical(rawLocale)
          if(locale.getName != "en_US") throw InvalidLocale(rawLocale)(index) // for now, we only allow en_US
          CreateDatasetMutation(index, locale.getName, getOption[String]("resource"))
        case other =>
          throw InvalidCommandFieldValue(originalObject, "c", JString(other))(index)
      }
      val user = get[String]("user")
      new CommandStream(streamType, user, remainingCommands.buffered)
    }

  def createCommandStream(index: Long, value: JValue, datasetId: DatasetId,
                          remainingCommands: Iterator[JValue]) =
    withObjectFields(index, value) { accessor =>
      import accessor._
      val command = get[String]("c")
      val streamType = command match {
        case "copy" =>
          val copyData = get[Boolean]("copy_data")
          val schemaHash = getOption[String]("schema")
          val dataVersion = getOption[Long]("data_version")
          CreateWorkingCopyMutation(index, datasetId, copyData, schemaHash, dataVersion)
        case "publish" =>
          val schemaHash = getOption[String]("schema")
          val keepSnapshot = getWithStrictDefault("keep_snapshot", true)
          val dataVersion = getOption[Long]("data_version")
          PublishWorkingCopyMutation(index, datasetId, keepSnapshot, schemaHash, dataVersion)
        case "drop" =>
          val schemaHash = getOption[String]("schema")
          val dataVersion = getOption[Long]("data_version")
          DropWorkingCopyMutation(index, datasetId, schemaHash, dataVersion)
        case "normal" =>
          val schemaHash = getOption[String]("schema")
          val dataVersion = getOption[Long]("data_version")
          NormalMutation(index, datasetId, schemaHash, dataVersion)
        case other =>
          throw InvalidCommandFieldValue(originalObject, "c", JString(other))(index)
      }
      val user = get[String]("user")
      new CommandStream(streamType, user, remainingCommands.buffered)
    }

  def mapToEvents[T](m: collection.Map[Int,T])(implicit codec: JsonEncode[T]): Iterator[JsonEvent] = {
    def elemToStream(kv: (Int, T)) =
      new BuiltUpIterator(
        Iterator.single(FieldEvent(kv._1.toString)(Position.Invalid)),
        JValueEventIterator(codec.encode(kv._2)))
    new BuiltUpIterator(
      Iterator.single(StartOfObjectEvent()(Position.Invalid)),
      m.iterator.flatMap(elemToStream),
      Iterator.single(EndOfObjectEvent()(Position.Invalid)))
  }

  def toEventStream(inserted: collection.Map[Int, JValue],
                    updated: collection.Map[Int, JValue],
                    deleted: collection.Map[Int, JValue],
                    errors: collection.Map[Int, JValue]) = {
    new BuiltUpIterator(
      Iterator(StartOfObjectEvent()(Position.Invalid), FieldEvent("inserted")(Position.Invalid)),
      mapToEvents(inserted),
      Iterator.single(FieldEvent("updated")(Position.Invalid)),
      mapToEvents(updated),
      Iterator.single(FieldEvent("deleted")(Position.Invalid)),
      mapToEvents(deleted),
      Iterator.single(FieldEvent("errors")(Position.Invalid)),
      mapToEvents(errors),
      Iterator.single(EndOfObjectEvent()(Position.Invalid)))
  }

  type UniverseWithProviders = Universe[CT, CV] with DatasetMutatorProvider
                                                with SchemaFinderProvider
                                                with DatasetMapReaderProvider

  def createScript(u: UniverseWithProviders, commandStream: Iterator[JValue]):
      ProcessCreationReturns = {
    if(commandStream.isEmpty) throw EmptyCommandStream()(0L)
    val commands = createCreateStream(0L, commandStream.next(), commandStream)
    val (datasetId, mutationResults) = runScript(u, commands)

    // Have to re-lookup copy info to get a valid lastModified from the DB...
    val copyInfo = u.datasetMapReader.latest(u.datasetMapReader.datasetInfo(datasetId).get)
    ProcessCreationReturns(datasetId, copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified, mutationResults)
  }

  def updateScript(u: UniverseWithProviders, datasetId: DatasetId, commandStream: Iterator[JValue]):
      ProcessMutationReturns = {
    if(commandStream.isEmpty) throw EmptyCommandStream()(0L)
    val commands = createCommandStream(0L, commandStream.next(), datasetId, commandStream)
    val (_, mutationResults) = runScript(u, commands)
    val copyInfo = u.datasetMapReader.latest(u.datasetMapReader.datasetInfo(datasetId).get)
    ProcessMutationReturns(copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified, mutationResults)
  }

  val jobCounter = new Counter(0)

  class JsonReportWriter(ctx: DatasetMutator[CT, CV]#MutationContext,
                         val firstJob: Long,
                         tmpFile: IndexedTempFile,
                         ignorableFailureTypes: Set[Class[_ <: Failure[_]]],
                         bySystemId: Boolean) extends ReportWriter[CV] {
    val primaryKey = ctx.primaryKey
    val systemId = ctx.systemId
    // deletes are "forced" by system id only when there is a user primary key column
    val deletesForcedBySystemId = bySystemId && systemId.systemId != primaryKey.systemId

    val jsonRepFor = jsonReps(ctx.copyInfo.datasetInfo)
    val pkRep = jsonRepFor(primaryKey.typ)
    val sidRep = jsonRepFor(systemId.typ)
    def rowIdRep(bySystemIdForced: Boolean) = if (bySystemIdForced) sidRep else pkRep
    val verRep = jsonRepFor(ctx.versionColumn.typ)
    @volatile var firstError: Option[Failure[CV]] = None
    var jobLimit = firstJob - 1

    def jsonifyId(id: CV, bySystemIdForced: Boolean) = rowIdRep(bySystemIdForced).toJValue(id)
    def jsonifyVersion(v: RowVersion) =
      verRep.toJValue(typeContext.makeValueFromRowVersion(v))

    def writeJson(job: Int, value: JValue) = synchronized {
      val stream = new java.io.OutputStreamWriter(tmpFile.newRecord(job), UTF_8)
      CompactJsonWriter.toWriter(stream, value)
      stream.close()
      jobLimit = Math.max(jobLimit, job)
    }

    def jsonifyUpsert(idAndVersion: IdAndVersion[CV], typ: String) = {
      JObject(Map(
        "typ" -> JString(typ),
        "id" -> jsonifyId(idAndVersion.id, idAndVersion.bySystemIdForced),
        "ver" -> jsonifyVersion(idAndVersion.version)
      ))
    }

    def jsonifyDelete(result: CV) = {
      JObject(Map(
        "typ" -> JString("delete"),
        "id" -> jsonifyId(result, deletesForcedBySystemId)
      ))
    }

    def jsonifyError(err: Failure[CV]) = err match {
      case NoPrimaryKey =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("no_primary_key")
        ))
      case NoSuchRowToDelete(id) =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("no_such_row_to_delete"),
          "id" -> jsonifyId(id, deletesForcedBySystemId)
        ))
      case NoSuchRowToUpdate(id, bySystemIdForced) =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("no_such_row_to_update"),
          "id" -> jsonifyId(id, bySystemIdForced)
        ))
      case VersionMismatch(id, expected, actual, bySystemIdForced) =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("version_mismatch"),
          "id" -> jsonifyId(id, bySystemIdForced),
          "expected" -> expected.map(jsonifyVersion).getOrElse(JNull),
          "actual" -> actual.map(jsonifyVersion).getOrElse(JNull)
        ))
      case VersionOnNewRow =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("version_on_new_row")
        ))
      case InsertInUpdateOnly(id, bySystemIdForced) =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("insert_in_update_only"),
          "id" -> jsonifyId(id, bySystemIdForced)
        ))
    }

    def inserted(job: Int, result: IdAndVersion[CV]) {
      writeJson(job, jsonifyUpsert(result, "insert"))
    }

    def updated(job: Int, result: IdAndVersion[CV]) {
      writeJson(job, jsonifyUpsert(result, "update"))
    }

    def deleted(job: Int, result: CV) {
      writeJson(job, jsonifyDelete(result))
    }

    def error(job: Int, result: Failure[CV]) {
      if(None == firstError && !ignorableFailureTypes.exists(_.isAssignableFrom(result.getClass)))
        firstError = Some(result)
      writeJson(job, jsonifyError(result))
    }

    def toJobRange: NumericRange[Long] =
      firstJob to jobLimit
  }

  private def runScript(u: Universe[CT, CV] with DatasetMutatorProvider with SchemaFinderProvider,
                        commands: CommandStream): (DatasetId, Seq[MutationScriptCommandResult]) = {
    def user = commands.user

    def doProcess(ctx: DatasetMutator[CT, CV]#MutationContext): (DatasetId, Seq[MutationScriptCommandResult]) = {
      val processor = new Processor(ctx)
      val events = processor.carryOutCommands(commands)
      (ctx.copyInfo.datasetInfo.systemId, events)
    }

    def checkDatasetState(index: Long, schemaHash: Option[String], expectedDataVersion: Option[Long], ctx: DatasetCopyContext[CT]) {
      for(givenSchemaHash <- schemaHash) {
        val realSchemaHash = u.schemaFinder.schemaHash(ctx)
        if(givenSchemaHash != realSchemaHash) {
          throw MismatchedSchemaHash(ctx.datasetInfo.systemId, u.schemaFinder.getSchema(ctx))(index)
        }
      }
      for(givenDataVersion <- expectedDataVersion) {
        val realDataVersion = ctx.datasetInfo.latestDataVersion
        if(givenDataVersion != realDataVersion) {
          throw MismatchedDataVersion(ctx.datasetInfo.systemId, realDataVersion)(index)
        }
      }
    }

    def process(index: Long, datasetId: DatasetId, mutator: DatasetMutator[CT, CV])
               (maybeCtx: mutator.CopyContext) = maybeCtx match {
      case mutator.CopyOperationComplete(ctx) =>
        doProcess(ctx)
      case error: mutator.CopyContextError =>
        processError(index, datasetId, mutator)(error)
    }

    def processError(index: Long, datasetId: DatasetId, mutator: DatasetMutator[CT, CV])(error: mutator.CopyContextError) = error match {
      case mutator.IncorrectLifecycleStage(currentStage, expectedStages) =>
        throw IncorrectLifecycleStage(datasetId, currentStage, expectedStages)(index)
      case mutator.DatasetDidNotExist() =>
        throw NoSuchDataset(datasetId)(index)
      case mutator.SecondariesNotUpToDate(stores) =>
        throw SecondaryStoresNotUpToDate(datasetId, stores)(index)
    }

    try {
      val mutator = u.datasetMutator
      commands.streamType match {
        case NormalMutation(idx, datasetId, schemaHash, expectedDataVersion) =>
          for(ctxOpt <- mutator.openDataset(user)(datasetId, checkDatasetState(idx, schemaHash, expectedDataVersion, _))) yield {
            val ctx = ctxOpt.getOrElse { throw NoSuchDataset(datasetId)(idx) }
            doProcess(ctx)
          }
        case CreateDatasetMutation(idx, localeName, resourceName) =>
          for(ctx <- u.datasetMutator.createDataset(user)(localeName, resourceName)) yield {
            val cis = ctx.addColumns(systemSchema.toSeq.map { case (col, colInfo) =>
              ctx.ColumnToAdd(col, colInfo.fieldName, colInfo.typ, physicalColumnBaseBase(col.underlying, systemColumn = true), colInfo.computationStrategy)
            })
            for(ci <- cis) {
              val ci2 =
                if(ci.userColumnId == systemIdColumnId) ctx.makeSystemPrimaryKey(ci)
                else ci
              if(ci2.userColumnId == versionColumnId) ctx.makeVersion(ci2)
            }
            doProcess(ctx)
          }
        case CreateWorkingCopyMutation(idx, datasetId, copyData, schemaHash, expectedDataVersion) =>
          mutator.createCopy(user)(datasetId, copyData = copyData,
                                   checkDatasetState(idx, schemaHash, expectedDataVersion, _)).map(process(idx, datasetId, mutator))
        case PublishWorkingCopyMutation(idx, datasetId, keepSnapshot, schemaHash, expectedDataVersion) =>
          mutator.publishCopy(user)(datasetId, keepSnapshot, checkDatasetState(idx, schemaHash, expectedDataVersion, _)).map(process(idx, datasetId, mutator))
        case DropWorkingCopyMutation(idx, datasetId, schemaHash, expectedDataVersion) =>
          mutator.dropCopy(user)(datasetId, checkDatasetState(idx, schemaHash, expectedDataVersion, _)).map {
            case cc: mutator.CopyContextError =>
              processError(idx, datasetId, mutator)(cc)
            case mutator.InitialWorkingCopy =>
              throw InitialCopyDrop(datasetId)(idx)
            case mutator.DropComplete =>
              if(commands.nextCommand().isDefined) {
                throw OperationAfterDrop(datasetId)(0)
              } else {
                (datasetId, Nil)
              }
          }
      }
    } catch {
      case e: DatasetIdInUseByWriterException =>
        throw CannotAcquireDatasetWriteLock(e.datasetId)(commands.streamType.index)
      case e: DatabaseInReadOnlyMode =>
        throw SystemInReadOnlyMode()(commands.streamType.index)
    }
  }

  class Processor(mutator: DatasetMutator[CT, CV]#MutationContext) {
    val jsonRepFor = jsonReps(mutator.copyInfo.datasetInfo)
    val labels = new mutable.HashMap[ColumnLabel, UserColumnId]

    def idFor(idx: Long, label: ColumnLabel) =
      labels.getOrElse(label, throw NoSuchColumnLabel(datasetId, label.label)(idx))

    val datasetId = mutator.copyInfo.datasetInfo.systemId
    def checkDDL(idx: Long) {
      if(!allowDdlOnPublishedCopies && mutator.copyInfo.lifecycleStage != LifecycleStage.Unpublished)
        throw IncorrectLifecycleStage(datasetId, mutator.copyInfo.lifecycleStage,
                                      Set(LifecycleStage.Unpublished))(idx)
    }

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

    private val pendingAdds = new mutable.ListBuffer[mutator.ColumnToAdd]
    private val pendingDrops = new mutable.ListBuffer[UserColumnId]

    def isExistingColumn(cid: UserColumnId) =
      (mutator.schema.iterator.map(_._2.userColumnId).contains(cid) ||
       pendingAdds.exists(_.userColumnId == cid)) &&
      !pendingDrops.contains(cid)

    // If some user column id is specified, will return true if the field name is a field name for some other column
    def isExistingFieldName(cfn: ColumnName, cid: Option[UserColumnId] = None) = {
      val pendingDropsSet = pendingDrops.toSet
      mutator.schema.iterator.filterNot { c => pendingDropsSet(c._2.userColumnId)}.map(_._2).exists { col =>
        col.fieldName == Some(cfn) && cid.forall(col.userColumnId != _)
      } || pendingAdds.exists { col => col.fieldName == Some(cfn) && cid.forall(col.userColumnId != _) }
    }

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
        mutator.dropColumns(pendingDrops.map { cid =>
          mutator.columnInfo(cid).getOrElse {
            sys.error("I verified column " + cid + " existed before adding it to the list for dropping?")
          }
        })
        val res = Vector.fill(pendingDrops.size) { MutationScriptCommandResult.Uninteresting }
        pendingDrops.clear()
        res
      } else {
        Vector.empty
      }

      addResults ++ dropResults
    }

    def convertStrategy(idx: Long, spec: ComputationStrategySpec): ComputationStrategyInfo = {
      val ComputationStrategySpec(strategyType, sourceColumnIds, parameters) = spec
      def idOf(id: ColumnIdSpec) = id.fold(idFor(idx, _), identity)
      // bit icky; using "label" here might not be distinctive enough
      val magicKey = "label"
      def convert(v: JValue): JValue = v match {
        case a: JAtom => a
        case JArray(arr) => JArray(arr.map(convert))
        case JObject(fields) if fields.size == 1 && fields.contains(magicKey) && fields(magicKey).isInstanceOf[JString] =>
          JString(idFor(idx, ColumnLabel(fields(magicKey).asInstanceOf[JString].string)).underlying)
        case JObject(fields) => JObject(fields.mapValues(convert))
      }

      ComputationStrategyInfo(strategyType, sourceColumnIds.map(idOf), JObject(parameters.mapValues(convert)))
    }

    def carryOutCommand(commands: CommandStream, cmd: Command): Seq[MutationScriptCommandResult] = {
      val pendingResults =
        if(!cmd.isInstanceOf[AddColumn] && pendingAdds.nonEmpty) flushPendingCommands()
        else if(!cmd.isInstanceOf[DropColumn] && pendingDrops.nonEmpty) flushPendingCommands()
        else Vector.empty

      def processCommand(cmd: Command): Seq[MutationScriptCommandResult] = cmd match {
        case AddColumn(idx, None, nameHint, fieldName, typName, computationStrategy) =>
          fieldName.foreach { fn => if(isExistingFieldName(fn)) throw FieldNameAlreadyExists(datasetId, fn)(idx) }

          val typ = nameForTypeOpt(typName).getOrElse {
            throw NoSuchType(typName)(idx)
          }
          checkDDL(idx)

          pendingAdds += mutator.ColumnToAdd(createId(), fieldName, typ, physicalColumnBaseBase(nameHint), computationStrategy.map(convertStrategy(idx, _)))
          Nil
        case ac@AddColumn(idx, Some(Left(label)), _, _, _, _) =>
          val id = createId()
          labels(label) = id
          processCommand(ac.copy(id = Some(Right(id))))
        case AddColumn(idx, Some(Right(id)), nameHint, fieldName, typName, computationStrategy) =>
          if(isSystemColumnId(id)) throw IllegalColumnId(id)(idx)
          if(isExistingColumn(id)) throw ColumnAlreadyExists(datasetId, id)(idx)
          fieldName.foreach { fn => if(isExistingFieldName(fn)) throw FieldNameAlreadyExists(datasetId, fn)(idx) }

          val typ = nameForTypeOpt(typName).getOrElse {
            throw NoSuchType(typName)(idx)
          }
          checkDDL(idx)
          pendingAdds += mutator.ColumnToAdd(id, fieldName, typ, physicalColumnBaseBase(nameHint), computationStrategy.map(convertStrategy(idx, _)))
          Nil
        case dc@DropColumn(idx, Left(label)) =>
          processCommand(dc.copy(id = Right(idFor(idx, label))))
        case DropColumn(idx, Right(id)) =>
          if(!isExistingColumn(id)) throw NoSuchColumn(datasetId, id)(idx)
          if(isSystemColumnId(id)) throw InvalidSystemColumnOperation(datasetId, id, DropColumnOp)(idx)
          if(mutator.columnInfo(id).get.isUserPrimaryKey) {
            val Seq(MutationScriptCommandResult.Uninteresting) = processCommand(DropRowId(idx, Right(id)))
          }
          checkDDL(idx)
          pendingDrops += id
          Nil
        case dcs@AddComputationStrategy(idx, Left(label), cs) =>
          processCommand(dcs.copy(id = Right(idFor(idx, label))))
        case AddComputationStrategy(idx, Right(id), cs) =>
          if(!isExistingColumn(id)) throw NoSuchColumn(datasetId, id)(idx)
          val col = mutator.columnInfo(id).getOrElse { sys.error("I just verified column " + id + " existed?") }
          val csi = convertStrategy(idx, cs)
          mutator.addComputationStrategy(col, csi)
          Seq(MutationScriptCommandResult.Uninteresting)
        case dcs@DropComputationStrategy(idx, Left(label)) =>
          processCommand(dcs.copy(id = Right(idFor(idx, label))))
        case DropComputationStrategy(idx, Right(id)) =>
          if(!isExistingColumn(id)) throw NoSuchColumn(datasetId, id)(idx)
          val col = mutator.columnInfo(id).getOrElse { sys.error("I just verified column " + id + " existed?") }
          if(col.computationStrategyInfo.isEmpty) throw  NoComputationStrategy(datasetId, id)(idx)
          if(isSystemColumnId(id)) throw InvalidSystemColumnOperation(datasetId, id, DropComputationStrategyOp)(idx)
          mutator.dropComputationStrategy(col)
          Seq(MutationScriptCommandResult.Uninteresting)
        case ufn@UpdateFieldName(idx, Left(label), _) =>
          processCommand(ufn.copy(id = Right(idFor(idx, label))))
        case UpdateFieldName(idx, Right(id), fieldName) =>
          if(!isExistingColumn(id)) throw NoSuchColumn(datasetId, id)(idx)
          if(isSystemColumnId(id)) throw InvalidSystemColumnOperation(datasetId, id, UpdateColumnNameOp)(idx)
          if(isExistingFieldName(fieldName, Some(id))) throw FieldNameAlreadyExists(datasetId, fieldName)(idx) // can update a column to have the same field name as before

          mutator.updateFieldName(mutator.columnInfo(id).getOrElse(sys.error("I just verified column " + id + " existed?")), fieldName)
          Seq(MutationScriptCommandResult.Uninteresting)
        case sri@SetRowId(idx, Left(label)) =>
          processCommand(sri.copy(id = Right(idFor(idx, label))))
        case SetRowId(idx, Right(id)) =>
          mutator.columnInfo(id) match {
            case Some(colInfo) =>
              for(pkCol <- mutator.schema.values.find(_.isUserPrimaryKey))
                throw PrimaryKeyAlreadyExists(datasetId, id, pkCol.userColumnId)(idx)
              if(isSystemColumnId(id)) throw InvalidSystemColumnOperation(datasetId, id, SetRowIdOp)(idx)
              try {
                checkDDL(idx)
                mutator.makeUserPrimaryKey(colInfo)
              } catch {
                case e: mutator.PrimaryKeyCreationException => e match {
                  case mutator.UnPKableColumnException(_, _) =>
                    throw InvalidTypeForPrimaryKey(datasetId, colInfo.userColumnId, typeNameFor(colInfo.typ))(idx)
                  case mutator.NullCellsException(c) =>
                    throw NullsInColumn(datasetId, colInfo.userColumnId)(idx)
                  case mutator.DuplicateCellsException(_) =>
                    throw DuplicateValuesInColumn(datasetId, colInfo.userColumnId)(idx)
                }
              }
            case None =>
              throw NoSuchColumn(datasetId, id)(idx)
          }
          Seq(MutationScriptCommandResult.Uninteresting)
        case dri@DropRowId(idx, Left(label)) =>
          processCommand(dri.copy(id = Right(idFor(idx, label))))
        case DropRowId(idx, Right(id)) =>
          mutator.columnInfo(id) match {
            case Some(colInfo) =>
              if(!colInfo.isUserPrimaryKey) throw NotPrimaryKey(datasetId, id)(idx)
              checkDDL(idx)
              mutator.unmakeUserPrimaryKey(colInfo)
            case None =>
              throw NoSuchColumn(datasetId, id)(idx)
          }
          Seq(MutationScriptCommandResult.Uninteresting)
        case CreateOrUpdateRollup(idx, name, soql) =>
          mutator.createOrUpdateRollup(name, soql)
          Seq(MutationScriptCommandResult.Uninteresting)
        case DropRollup(idx, name) =>
          mutator.dropRollup(name) match {
            case Some(info) => Seq(MutationScriptCommandResult.Uninteresting)
            case None => throw NoSuchRollup(name)(idx)
          }

        case RowData(idx, truncate, mergeReplace, nonFatalRowErrors, updateOnly, bySystemId) =>
          if(truncate) mutator.truncate()
          val data = processRowData(idx, commands.rawCommandStream, nonFatalRowErrors, updateOnly = updateOnly, bySystemId = bySystemId, mutator, mergeReplace)
          Seq(MutationScriptCommandResult.RowData(data.toJobRange))
      }

      val newResults = processCommand(cmd)

      pendingResults ++ newResults
    }

    def processRowData(idx: Long,
                       rows: BufferedIterator[JValue],
                       nonFatalRowErrors: Set[Class[_ <: Failure[_]]],
                       updateOnly: Boolean,
                       bySystemId: Boolean,
                       mutator: DatasetMutator[CT,CV]#MutationContext,
                       mergeReplace: MergeReplace): JsonReportWriter = {
      import mutator._
      class UnknownCid(val job: Int, val cid: UserColumnId) extends Exception
      def onUnknownColumn(cid: UserColumnId) {
        throw new UnknownCid(jobCounter(), cid)
      }
      val plan = new RowDecodePlan(schema, jsonRepFor, typeNameFor,
                   (v: CV) => if(typeContext.isNull(v)) None else Some(typeContext.makeRowVersionFromValue(v)),
                   onUnknownColumn)
      try {
        val reportWriter = new JsonReportWriter(mutator, jobCounter.peek, indexedTempFile, nonFatalRowErrors, bySystemId = bySystemId)
        def checkForError() {
          for(error <- reportWriter.firstError) {
            val pk = schema.values.find(_.isUserPrimaryKey).
                                   orElse(schema.values.find(_.isSystemPrimaryKey)).
                                   getOrElse { sys.error("No primary key on this dataset?") }
            val trueError = error.map(jsonRepFor(pk.typ).toJValue)
            val jsonizer = { (rv: RowVersion) =>
              jsonRepFor(mutator.versionColumn.typ).toJValue(typeContext.makeValueFromRowVersion(rv))
            }
            throw UpsertError(mutator.copyInfo.datasetInfo.systemId, trueError, jsonizer)(idx)
          }
        }
        val it = new Iterator[RowDataUpdateJob] {
          def hasNext = rows.hasNext && JNull != rows.head
          def next() = {
            if(!hasNext) throw new NoSuchElementException
            checkForError()
            plan(rows.next()) match {
              case Right(row) => UpsertJob(jobCounter(), row)
              case Left((id, version)) => DeleteJob(jobCounter(), id, version)
            }
          }
        }
        mutator.upsert(it, reportWriter, replaceUpdatedRows = mergeReplace == Replace, updateOnly = updateOnly, bySystemId = bySystemId)
        if(rows.hasNext && JNull == rows.head) rows.next()
        checkForError()
        reportWriter
      } catch {
        case e: plan.BadDataException => e match {
          case plan.BadUpsertCommandException(value) =>
            throw InvalidUpsertCommand(mutator.copyInfo.datasetInfo.systemId, value)(idx, jobCounter.lastValue)
          case plan.UninterpretableFieldValue(column, value, columnType)  =>
            throw InvalidValue(mutator.copyInfo.datasetInfo.systemId, column, typeNameFor(columnType),
                               value)(idx, jobCounter.lastValue)
        }
        case e: UnknownCid =>
          throw UnknownColumnId(mutator.copyInfo.datasetInfo.systemId, e.cid)(idx, e.job)
      }
    }
  }
}
