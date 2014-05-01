package com.socrata.datacoordinator.service.mutator

import com.socrata.datacoordinator.truth.universe.{DatasetMapReaderProvider, SchemaFinderProvider, DatasetMutatorProvider, Universe}
import org.joda.time.DateTime
import com.rojoma.json.ast._
import com.socrata.datacoordinator.id.{RowVersion, UserColumnId, DatasetId}

import MutatorLib._
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.{Counter, IndexedTempFile}
import com.socrata.datacoordinator.truth.{TypeContext, DatasetMutator}
import com.socrata.datacoordinator.service.RowDecodePlan
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.environment.TypeName
import com.rojoma.json.io.{JsonReader, FusedBlockJsonEventIterator, CompactJsonWriter}
import java.nio.charset.StandardCharsets
import com.socrata.datacoordinator.truth.loader.NoSuchRowToDelete
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.truth.loader.IdAndVersion
import com.socrata.datacoordinator.truth.loader.NoSuchRowToUpdate
import com.socrata.datacoordinator.truth.loader.VersionMismatch
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.rojoma.simplearm.util._
import java.io.{File, InputStreamReader}

trait DMLMutatorCommon[CT, CV] {
  def jsonReps(di: DatasetInfo): CT => JsonColumnRep[CT, CV]
  def typeContext: TypeContext[CT, CV]
  def indexedTempFileIndexBufSize: Int
  def indexedTempFileDataBufSize: Int
  def indexedTempFileTempDir: File
}

object DMLMutator {
  sealed abstract class MergeReplace
  case object Merge extends MergeReplace
  case object Replace extends MergeReplace
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
}

class DMLMutator[CT, CV](common: DMLMutatorCommon[CT, CV]) {
  import DMLMutator._
  import common._

  private case class UpsertHeader(user: String, schemaHash: Option[String], truncate: Boolean, mergeReplace: MergeReplace, nonFatalRowErrors: Set[Class[_ <: Failure[_]]])
  private def getUpsertHeader(value: JValue): UpsertHeader = {
    withObjectFields(value, HeaderError) { accessor =>
      import accessor._
      val user = get[String]("user")
      val schemaHash = getOption[String]("schemaHash")
      val truncate = getWithStrictDefault("truncate", false)
      val mergeReplace = getWithStrictDefault[MergeReplace]("update", Merge)
      val nonFatalRowErrors = getOption[Seq[String]]("nonfatal_row_errors").getOrElse {
        if(getWithStrictDefault("fatal_row_errors", true)) Seq.empty[String] else Failure.allFailures.keys
      }
      val nonFatalRowErrorsClasses = nonFatalRowErrors.map { nfe =>
        Failure.allFailures.getOrElse(nfe, HeaderError.invalidValue(originalObject, "nonfatal_row_errors", JString(nfe)))
      }.toSet
      UpsertHeader(user, schemaHash, truncate, mergeReplace, nonFatalRowErrorsClasses)
    }
  }

  def upsertScript(u: Universe[CT, CV] with DatasetMutatorProvider with SchemaFinderProvider with DatasetMapReaderProvider, datasetId: DatasetId, commandStream: Iterator[JValue]): Managed[(Long, DateTime, Iterator[JValue])] = {
    new SimpleArm[(Long, DateTime, Iterator[JValue])] {
      override def flatMap[A](f: ((Long, DateTime, Iterator[JValue])) => A): A = {
        using(new IndexedTempFile(indexedTempFileIndexBufSize, indexedTempFileDataBufSize, indexedTempFileTempDir)) { indexedTempFile =>
          translatingCannotWriteExceptions {
            if(commandStream.isEmpty) throw EmptyCommandStream()
            val upsertHeader = getUpsertHeader(commandStream.next())
            val report = for(ctxOpt <- u.datasetMutator.openDataset(upsertHeader.user)(datasetId, checkHash(u, upsertHeader.schemaHash, _))) yield {
              val ctx = ctxOpt.getOrElse { throw NoSuchDataset(datasetId) }
              val processor = new Processor(ctx, indexedTempFile)
              processor.processRowData(commandStream.buffered, upsertHeader.nonFatalRowErrors, ctx, upsertHeader.mergeReplace)
            }
            val copyInfo = u.datasetMapReader.latest(u.datasetMapReader.datasetInfo(datasetId).get)

            val tempIterator = new Iterator[JValue] {
              var current = 0L
              def hasNext = current <= report.jobLimit
              def next() = {
                indexedTempFile.readRecord(current) match {
                  case Some(in) =>
                    current += 1
                    JsonReader.fromEvents(new FusedBlockJsonEventIterator(new InputStreamReader(in, StandardCharsets.UTF_8)))
                  case None =>
                    throw new NoSuchElementException
                }
              }
            }

            f((copyInfo.dataVersion, copyInfo.lastModified, tempIterator))
          }
        }
      }
    }
  }

  class JsonReportWriter(ctx: DatasetMutator[CT, CV]#MutationContext, firstJob: Long, tmpFile: IndexedTempFile, ignorableFailureTypes: Set[Class[_ <: Failure[_]]]) extends ReportWriter[CV] {
    val jsonRepFor = jsonReps(ctx.copyInfo.datasetInfo)
    val pkRep = jsonRepFor(ctx.primaryKey.typ)
    val verRep = jsonRepFor(ctx.versionColumn.typ)
    @volatile var firstError: Option[(Int, Failure[CV])] = None
    var jobLimit = firstJob - 1

    def jsonifyId(id: CV) = pkRep.toJValue(id)
    def jsonifyVersion(v: RowVersion) =
      verRep.toJValue(typeContext.makeValueFromRowVersion(v))

    def writeJson(job: Int, value: JValue) = synchronized {
      val stream = new java.io.OutputStreamWriter(tmpFile.newRecord(job), StandardCharsets.UTF_8)
      CompactJsonWriter.toWriter(stream, value)
      stream.close()
      jobLimit = Math.max(jobLimit, job)
    }

    def jsonifyUpsert(idAndVersion: IdAndVersion[CV], typ: String) = {
      JObject(Map(
        "typ" -> JString(typ),
        "id" -> jsonifyId(idAndVersion.id),
        "ver" -> jsonifyVersion(idAndVersion.version)
      ))
    }

    def jsonifyDelete(result: CV) = {
      JObject(Map(
        "typ" -> JString("delete"),
        "id" -> jsonifyId(result)
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
          "id" -> jsonifyId(id)
        ))
      case NoSuchRowToUpdate(id) =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("no_such_row_to_update"),
          "id" -> jsonifyId(id)
        ))
      case VersionMismatch(id, expected, actual) =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("version_mismatch"),
          "id" -> jsonifyId(id),
          "expected" -> expected.map(jsonifyVersion).getOrElse(JNull),
          "actual" -> actual.map(jsonifyVersion).getOrElse(JNull)
        ))
      case VersionOnNewRow =>
        JObject(Map(
          "typ" -> JString("error"),
          "err" -> JString("version_on_new_row")
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
        firstError = Some(job -> result)
      writeJson(job, jsonifyError(result))
    }
  }

  private class Processor(mutator: DatasetMutator[CT, CV]#MutationContext, indexedTempFile: IndexedTempFile) {
    val jsonRepFor = jsonReps(mutator.copyInfo.datasetInfo)

    val jobCounter = new Counter(0)

    def typeNameFor(typ: CT): TypeName =
      typeContext.typeNamespace.userTypeForType(typ)

    def processRowData(rows: BufferedIterator[JValue], nonFatalRowErrors: Set[Class[_ <: Failure[_]]], mutator: DatasetMutator[CT,CV]#MutationContext, mergeReplace: MergeReplace): JsonReportWriter = {
      import mutator._
      class UnknownCid(val job: Int, val cid: UserColumnId) extends Exception
      def onUnknownColumn(cid: UserColumnId) {
        throw new UnknownCid(jobCounter(), cid)
      }
      val plan = new RowDecodePlan(schema, jsonRepFor, typeNameFor, (v: CV) => if(typeContext.isNull(v)) None else Some(typeContext.makeRowVersionFromValue(v)), onUnknownColumn)
      try {
        val reportWriter = new JsonReportWriter(mutator, jobCounter.peek, indexedTempFile, nonFatalRowErrors)
        def checkForError() {
          for((errorJob, error) <- reportWriter.firstError) {
            val pk = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
              sys.error("No primary key on this dataset?")
            }
            val trueError = error.map(jsonRepFor(pk.typ).toJValue)
            val jsonizer = { (rv: RowVersion) =>
              jsonRepFor(mutator.versionColumn.typ).toJValue(typeContext.makeValueFromRowVersion(rv))
            }
            throw UpsertError(mutator.copyInfo.datasetInfo.systemId, trueError, jsonizer, errorJob)
          }
        }
        val it = new Iterator[RowDataUpdateJob] {
          def hasNext = rows.hasNext
          def next() = {
            if(!hasNext) throw new NoSuchElementException
            checkForError()
            plan(rows.next()) match {
              case Right(row) => UpsertJob(jobCounter(), row)
              case Left((id, version)) => DeleteJob(jobCounter(), id, version)
            }
          }
        }
        mutator.upsert(it, reportWriter, replaceUpdatedRows = mergeReplace == Replace)
        checkForError()
        reportWriter
      } catch {
        case e: plan.BadDataException => e match {
          case plan.BadUpsertCommandException(value) =>
            throw InvalidUpsertCommand(mutator.copyInfo.datasetInfo.systemId, value, jobCounter.lastValue)
          case plan.UninterpretableFieldValue(column, value, columnType)  =>
            throw InvalidValue(mutator.copyInfo.datasetInfo.systemId, column, typeNameFor(columnType), value, jobCounter.lastValue)
        }
        case e: UnknownCid =>
          throw UnknownColumnId(mutator.copyInfo.datasetInfo.systemId, e.cid, e.job)
      }
    }
  }
}
