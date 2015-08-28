package com.socrata.datacoordinator.service

import com.ibm.icu.text.Normalizer
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.io._
import com.rojoma.json.v3.util.{JsonArrayIterator, AutomaticJsonCodecBuilder, JsonKey, JsonUtil}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{RollupName, UserColumnId, DatasetId}
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata.{RollupInfo, SchemaField, Schema}
import com.socrata.datacoordinator.truth.Snapshot
import com.socrata.datacoordinator.util.collection.{UserColumnIdMap, UserColumnIdSet}
import com.socrata.datacoordinator.util.IndexedTempFile
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing._
import com.socrata.http.server.util.handlers.{ThreadRenamingHandler, NewLoggingHandler}
import com.socrata.http.server.util.{StrongEntityTag, EntityTag, Precondition, ErrorAdapter}
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server._
import com.socrata.soql.environment.TypeName
import com.socrata.thirdparty.metrics.{SocrataHttpSupport, Metrics, MetricsOptions, MetricsReporter}
import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import javax.activation.{MimeTypeParseException, MimeType}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object Service {
  type processMutationFunc = (DatasetId, Iterator[JValue], IndexedTempFile) => ProcessMutationReturns
  type datasetContentsFunc = Either[Schema, (EntityTag, Seq[SchemaField],
                                             Option[UserColumnId], String,
                                             Long, Iterator[Array[JValue]])] => Unit

  private val JsonContentType = "application/json; charset=utf-8"

  private val TextContentType = "Content-type: text/plain; charset=utf-8"

  val numThreads = new AtomicInteger
}

import Service._

/**
 * The main HTTP REST resource servicing class for the data coordinator.
 * It should probably be broken up into multiple classes per resource.
 */
class Service(serviceConfig: ServiceConfig,
              processMutation: (DatasetId, Iterator[JValue], IndexedTempFile) => ProcessMutationReturns,
              processCreation: (Iterator[JValue], IndexedTempFile) => ProcessCreationReturns,
              getSchema: DatasetId => Option[Schema],
              getRollups: DatasetId => Option[Seq[RollupInfo]],
              datasetContents: (DatasetId, Option[String], CopySelector, Option[UserColumnIdSet],
                                Option[Long], Option[Long], Precondition, Option[DateTime], Boolean) =>
                               datasetContentsFunc => Exporter.Result[Unit],

              secondaries: Set[String],
              datasetsInStore: (String) => Map[DatasetId, Long],
              versionInStore: (String, DatasetId) => Option[Long],
              ensureInSecondary: (String, DatasetId) => Unit,
              ensureInSecondaryGroup: (String, DatasetId) => Unit,
              secondariesOfDataset: DatasetId => Map[String, Long],
              datasets: () => Seq[DatasetId],
              deleteDataset: DatasetId => DatasetDropper.Result,
              commandReadLimit: Long,
              formatDatasetId: DatasetId => String,
              parseDatasetId: String => Option[DatasetId],
              tempFileProvider: () => IndexedTempFile) extends Metrics
{
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def norm(s: String) = Normalizer.normalize(s, normalizationMode)

  def normalizeJson(token: JsonEvent): JsonEvent = {
    def position(t: JsonEvent) = t.positionedAt(token.position)
    token match {
      case StringEvent(s) =>
        position(StringEvent(norm(s))(token.position))
      case FieldEvent(s) =>
        position(FieldEvent(norm(s))(token.position))
      case IdentifierEvent(s) =>
        position(IdentifierEvent(norm(s))(token.position))
      case other =>
        other
    }
  }

  def notFoundError(datasetId: String) =
    err(NotFound, "update.dataset.does-not-exist",
      "dataset" -> JString(datasetId))

  def writeLockError(datasetId: DatasetId) =
    err(Conflict, "update.dataset.temporarily-not-writable",
      "dataset" -> JString(formatDatasetId(datasetId)))

  object SecondaryManifestsResource extends SodaResource {
    override val get = doGetSecondaries _

    def doGetSecondaries(req: HttpRequest): HttpResponse =
      OK ~> Json(secondaries.toSeq)
  }

  case class SecondaryManifestResource(storeId: String) extends SodaResource {
    override def get = doGetSecondaryManifest

    def doGetSecondaryManifest(req: HttpRequest): HttpResponse = {
      if(!secondaries(storeId)) return NotFound
      val ds = datasetsInStore(storeId)
      val dsConverted = ds.foldLeft(Map.empty[String, Long]) { (acc, kv) =>
        acc + (formatDatasetId(kv._1) -> kv._2)
      }
      OK ~> Json(dsConverted)
    }
  }

  case class DatasetSecondaryStatusResource(storeId: String, datasetId: DatasetId) extends SodaResource {
    override def get = doGetDataVersionInSecondary
    override def post = doUpdateVersionInSecondary

    def doGetDataVersionInSecondary(req: HttpRequest): HttpResponse = {
      if(!secondaries(storeId)) return NotFound
      versionInStore(storeId, datasetId) match {
        case Some(v) =>
          OK ~> Json(Map("version" -> v))
        case None =>
          NotFound
      }
    }

    def doUpdateVersionInSecondary(req: HttpRequest): HttpResponse = {
      val defaultSecondaryGroups: Set[String] = serviceConfig.secondary.defaultGroups
      val groupRe = "_(.*)_".r
      storeId match {
        case "_DEFAULT_" => defaultSecondaryGroups.foreach(ensureInSecondaryGroup(_, datasetId))
        case groupRe(g) if serviceConfig.secondary.groups.contains(g) => ensureInSecondaryGroup(g, datasetId)
        case secondary if secondaries(storeId) => ensureInSecondary(secondary, datasetId)
        case _ => return NotFound
      }
      OK
    }
  }

  case class SecondariesOfDatasetResource(datasetId: DatasetId) extends SodaResource {
    override def get = doGetSecondariesOfDataset

    def doGetSecondariesOfDataset(req: HttpRequest): HttpResponse = {
      val ss = secondariesOfDataset(datasetId)
      OK ~> Json(ss)
    }
  }

  class ReaderExceededBound(val bytesRead: Long) extends Exception
  class BoundedReader(underlying: Reader, bound: Long) extends Reader {
    private var count = 0L
    private def inc(n: Int) {
      count += n
      if(count > bound) throw new ReaderExceededBound(count)
    }

    override def read() =
      underlying.read() match {
        case -1 => -1
        case n => inc(1); n
      }

    def read(cbuf: Array[Char], off: Int, len: Int): Int =
      underlying.read(cbuf, off, len) match {
        case -1 => -1
        case n => inc(n); n
      }

    def close() {
      underlying.close()
    }

    def resetCount() {
      count = 0
    }
  }

  def jsonStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[HttpResponse, (Iterator[JsonEvent], () => Unit)] = {
    val nullableContentType = req.getContentType
    if(nullableContentType == null)
      return Left(err(BadRequest, "req.content-type.missing"))
    val contentType =
      try { new MimeType(nullableContentType) }
      catch { case _: MimeTypeParseException =>
        return Left(err(BadRequest, "req.content-type.unparsable",
          "content-type" -> JString(nullableContentType)))
      }
    if(!contentType.`match`("application/json")) {
      return Left(err(UnsupportedMediaType, "req.content-type.not-json",
        "content-type" -> JString(contentType.toString)))
    }
    val reader =
      try { req.getReader }
      catch { case _: UnsupportedEncodingException =>
        return Left(err(UnsupportedMediaType, "req.content-type.unknown-charset",
          "content-type" -> JString(req.getContentType.toString)))
      }
    val boundedReader = new BoundedReader(reader, approximateMaxDatumBound)
    Right((new FusedBlockJsonEventIterator(boundedReader).map(normalizeJson), boundedReader.resetCount _))
  }

  def err(codeSetter: HttpResponse, errorCode: String, data: (String, JValue)*): HttpResponse = {
    val response = JObject(Map(
      "errorCode" -> JString(errorCode),
      "data" -> JObject(data.toMap)
    ))
    log.info(errorCode)
    codeSetter ~> Write(JsonContentType) { w =>
      JsonUtil.writeJson(w, response, pretty = true, buffer = true)
    }
  }

  def mismatchedSchema(code: String, name: DatasetId, schema: Schema) =
    err(Conflict, code,
      "dataset" -> JString(formatDatasetId(name)),
      "schema" -> jsonifySchema(schema))

  // Repeatedly tries to get a new thread by bumping numThreads, using
  // compareAndSet to make sure our value is valid
  @annotation.tailrec
  private def tryGetMutationThread(timeoutMs: Long, waitInterval: Int = 100): Boolean = {
    if (timeoutMs <= 0) {
      log.info("tryGetMutationThread: timed out")
      false
    } else {
      val currThreads = numThreads.get()
      if (currThreads >= serviceConfig.maxMutationThreads ||
          !numThreads.compareAndSet(currThreads, currThreads + 1)) {
        if (currThreads >= serviceConfig.maxMutationThreads)
          log.info(s"tryGetMutationThread: too many threads ($currThreads), waiting for some to finish")
        Thread sleep waitInterval
        tryGetMutationThread(timeoutMs - waitInterval, waitInterval)
      } else {
        true
      }
    }
  }

  def withMutationScriptResults[T](f: => HttpResponse): HttpResponse = {
    if (!tryGetMutationThread(serviceConfig.mutationResourceTimeout.toMillis)) {
      return err(ServiceUnavailable, "mutation.threads.maxed-out")
    }
    try {
      f
    } catch {
      case e: ReaderExceededBound =>
        return err(RequestEntityTooLarge, "req.body.command-too-large",
          "bytes-without-full-datum" -> JNumber(e.bytesRead))
      case r: JsonReaderException =>
        return err(BadRequest, "req.body.malformed-json",
          "row" -> JNumber(r.row),
          "column" -> JNumber(r.column))
      case e: Mutator.MutationException =>
        def colErr(msg: String, dataset: DatasetId, colId: UserColumnId, resp: HttpResponse = BadRequest) = {
          import scala.language.reflectiveCalls
          err(resp, msg,
            "dataset" -> JString(formatDatasetId(dataset)),
            "column" -> JsonEncode.toJValue(colId))
        }
        e match {
          case Mutator.EmptyCommandStream() =>
            err(BadRequest, "req.script.header.missing")
          case Mutator.CommandIsNotAnObject(value) =>
            err(BadRequest, "req.script.command.non-object",
              "value" -> value)
          case Mutator.MissingCommandField(obj, field) =>
            err(BadRequest, "req.script.command.missing-field",
              "object" -> obj,
              "field" -> JString(field))
          case Mutator.MismatchedSchemaHash(name, schema) =>
            mismatchedSchema("req.script.header.mismatched-schema", name, schema)
          case Mutator.InvalidCommandFieldValue(obj, field, value) =>
            err(BadRequest, "req.script.command.invalid-field",
              "object" -> obj,
              "field" -> JString(field),
              "value" -> value)
          case Mutator.NoSuchDataset(name) =>
            notFoundError(formatDatasetId(name))
          case Mutator.NoSuchRollup(name) =>
            err(NotFound, "delete.rollup.does-not-exist",
              "rollup" -> JString(name.underlying))
          case Mutator.CannotAcquireDatasetWriteLock(name) =>
            writeLockError(name)
          case Mutator.SystemInReadOnlyMode() =>
            err(ServiceUnavailable, "update.read-only-mode")
          case Mutator.InvalidLocale(locale) =>
            err(BadRequest, "create.dataset.invalid-locale",
              "locale" -> JString(locale))
          case Mutator.IncorrectLifecycleStage(name, currentStage, expectedStage) =>
            err(Conflict, "update.dataset.invalid-state",
              "dataset" -> JString(formatDatasetId(name)),
              "actual-state" -> JString(currentStage.name),
              "expected-state" -> JArray(expectedStage.toSeq.map(_.name).map(JString)))
          case Mutator.InitialCopyDrop(name) =>
            err(Conflict, "update.dataset.initial-copy-drop",
              "dataset" -> JString(formatDatasetId(name)))
          case Mutator.OperationAfterDrop(name) =>
            err(BadRequest, "update.dataset.operation-after-drop",
              "dataset" -> JString(formatDatasetId(name)))
          case Mutator.ColumnAlreadyExists(dataset, name) =>
            colErr("update.column.exists", dataset, name, Conflict)
          case Mutator.IllegalColumnId(id) =>
            err(BadRequest, "update.column.illegal-id",
              "id" -> JsonEncode.toJValue(id))
          case Mutator.NoSuchType(typeName) =>
            err(BadRequest, "update.type.unknown",
              "type" -> JString(typeName.name))
          case Mutator.NoSuchColumn(dataset, name) =>
            colErr("update.column.not-found", dataset, name)
          case Mutator.InvalidSystemColumnOperation(dataset, name, _) =>
            colErr("update.column.system", dataset, name)
          case Mutator.PrimaryKeyAlreadyExists(datasetName, userColumnId, existingColumn) =>
            err(BadRequest, "update.row-identifier.already-set",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "column" -> JsonEncode.toJValue(userColumnId),
              "existing-column" -> JsonEncode.toJValue(userColumnId))
          case Mutator.InvalidTypeForPrimaryKey(datasetName, userColumnId, typeName) =>
            err(BadRequest, "update.row-identifier.invalid-type",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "column" -> JsonEncode.toJValue(userColumnId),
              "type" -> JString(typeName.name))
          case Mutator.DeleteRowIdentifierNotAllowed(datasetId, columnId) =>
            colErr("update.row-identifier.delete", datasetId, columnId)
          case Mutator.DuplicateValuesInColumn(dataset, name) =>
            colErr("update.row-identifier.duplicate-values", dataset, name)
          case Mutator.NullsInColumn(dataset, name) =>
            colErr("update.row-identifier.null-values", dataset, name)
          case Mutator.NotPrimaryKey(dataset, name) =>
            colErr("update.row-identifier.not-row-identifier", dataset, name)
          case Mutator.InvalidUpsertCommand(datasetName, value) =>
            err(BadRequest, "update.script.row-data.invalid-value",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "value" -> value)
          case Mutator.UnknownColumnId(datasetName, cid) =>
            err(BadRequest, "update.row.unknown-column",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "column" -> JsonEncode.toJValue(cid))
          case Mutator.InvalidValue(datasetName, userColumnId, typeName, value) =>
            err(BadRequest, "update.row.unparsable-value",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "column" -> JsonEncode.toJValue(userColumnId),
              "type" -> JString(typeName.name),
              "value" -> value)
          case Mutator.UpsertError(datasetName, NoPrimaryKey, _) =>
            err(BadRequest, "update.row.no-id",
              "dataset" -> JString(formatDatasetId(datasetName)))
          case Mutator.UpsertError(datasetName, NoSuchRowToDelete(id), _) =>
            err(BadRequest, "update.row.no-such-id",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "value" -> id)
          case Mutator.UpsertError(datasetName, NoSuchRowToUpdate(id),_ ) =>
            err(BadRequest, "update.row.no-such-id",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "value" -> id)
          case Mutator.UpsertError(datasetName, VersionMismatch(id, expected, actual), rowVersionToJson) =>
            err(BadRequest, "update.row-version-mismatch",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "value" -> id,
              "expected" -> expected.map(rowVersionToJson).getOrElse(JNull),
              "actual" -> actual.map(rowVersionToJson).getOrElse(JNull))
          case Mutator.UpsertError(datasetName, VersionOnNewRow, _) =>
            err(BadRequest, "update.version-on-new-row",
              "dataset" -> JString(formatDatasetId(datasetName)))
        }
    } finally {
      // Decrease the thread count
      numThreads.decrementAndGet()
    }
  }

  private def writeResult(o: OutputStream, r: MutationScriptCommandResult, tmp: IndexedTempFile) {
    r match {
      case MutationScriptCommandResult.ColumnCreated(id, typname) =>
        o.write(CompactJsonWriter.toString(JObject(Map("id" -> JsonEncode.toJValue(id), "type" -> JString(typname.name)))).getBytes(UTF_8))
      case MutationScriptCommandResult.Uninteresting =>
        o.write('{')
        o.write('}')
      case MutationScriptCommandResult.RowData(jobs) =>
        o.write('[')
        jobs.foreach(new Function1[Long, Unit] {
          var didOne = false
          def apply(job: Long) {
            if(didOne) o.write(',')
            else didOne = true
            tmp.readRecord(job).get.writeTo(o)
          }
        })
        o.write(']')
    }
  }

  trait SodaResource extends SimpleResource

  case class NotFoundDatasetResource(datasetIdRaw: String) extends SodaResource {
    override def post = if(datasetIdRaw == "") doCreateDataset else notFound
    override def delete = notFound
    override def get = if(datasetIdRaw == "") doListDatasets else notFound

    val dateTimeFormat = ISODateTimeFormat.dateTime
    def notFound = (_: Any) => notFoundError(datasetIdRaw)

    def doCreateDataset(req: HttpRequest)(resp: HttpServletResponse) {
      using(tempFileProvider()) { tmp =>
        val responseBuilder = withMutationScriptResults {
          jsonStream(req.servletRequest, commandReadLimit) match {
            case Right((events, boundResetter)) =>
              val iteratorOrError = try {
                Right(JsonArrayIterator[JValue](events))
              } catch { case _: JsonBadParse =>
                Left(err(BadRequest, "req.body.not-json-array"))
              }
              iteratorOrError match {
                case Right(iterator) =>
                  val ProcessCreationReturns(dataset, dataVersion, lastModified, result) = processCreation(iterator.map { ev => boundResetter(); ev }, tmp)
                  OK ~>
                    Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastModified)) ~>
                    Header("X-SODA2-Truth-Version", dataVersion.toString) ~>
                    ContentType(JsonContentType) ~>
                    Stream { w =>
                      val bw = new BufferedOutputStream(w)
                      bw.write('[')
                      bw.write(JString(formatDatasetId(dataset)).toString.getBytes(UTF_8))
                      for(r <- result) {
                        bw.write(',')
                        writeResult(bw, r, tmp)
                      }
                      bw.write(']')
                      bw.flush()
                      log.debug("Non-linear index seeks: {}", tmp.stats.nonLinearIndexSeeks)
                      log.debug("Non-linear data seeks: {}", tmp.stats.nonLinearDataSeeks)
                    }
                case Left(error) =>
                  error
              }
            case Left(response) =>
              response
          }
        }
        responseBuilder(resp)
      }
    }

    def doListDatasets(req: HttpRequest): HttpResponse = {
      val ds = datasets()
      OK ~> Write(JsonContentType) { w =>
        val bw = new BufferedWriter(w)
        val jw = new CompactJsonWriter(bw)
        bw.write('[')
        var didOne = false
        for(dsid <- ds) {
          if(didOne) bw.write(',')
          else didOne = true
          jw.write(JString(formatDatasetId(dsid)))
        }
        bw.write(']')
        bw.flush()
      }
    }
  }

  case class DatasetSchemaResource(datasetId: DatasetId) extends SodaResource {
    override def get = { (req: HttpRequest) =>
      val result = for {
        schema <- getSchema(datasetId)
      } yield {
        OK ~> Write(JsonContentType) { w =>
          JsonUtil.writeJson(w, jsonifySchema(schema))
        }
      }
      result.getOrElse(notFoundError(formatDatasetId(datasetId)))
    }
  }

  case class DatasetRollupResource(datasetId: DatasetId) extends SodaResource {
    override def get = { (req: HttpRequest) =>
      val result = for {
        rollups <- getRollups(datasetId)
      } yield {
          OK ~> Write(JsonContentType) { w =>
            JsonUtil.writeJson(w, rollups.map { r => r.unanchored })
          }
        }
      result.getOrElse(notFoundError(formatDatasetId(datasetId)))
    }
  }

  val mutateRate = metrics.meter("mutation-rate", "rows")

  case class DatasetResource(datasetId: DatasetId) extends SodaResource {
    override def post = doMutation
    override def delete = doDeleteDataset
    override def get = doExportFile

    val dateTimeFormat = ISODateTimeFormat.dateTime

    def doMutation(req: HttpRequest)(resp: HttpServletResponse) {
      using(tempFileProvider()) { tmp =>
        val responseBuilder = withMutationScriptResults {
          jsonStream(req.servletRequest, commandReadLimit) match {
            case Right((events, boundResetter)) =>
              val iteratorOrError = try {
                Right(JsonArrayIterator[JValue](events))
              } catch { case _: JsonBadParse =>
                Left(err(BadRequest, "req.body.not-json-array"))
              }
              iteratorOrError match {
                case Right(iterator) =>
                  val ProcessMutationReturns(copyNumber, version, lastModified, result) =
                    processMutation(datasetId,
                                    iterator.map { ev => boundResetter(); mutateRate.mark(); ev },
                                    tmp)
                  OK ~>
                    ContentType(JsonContentType) ~>
                    Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastModified)) ~>
                    Header("X-SODA2-Truth-Version", version.toString) ~>
                    Header("X-SODA2-Truth-Copy-Number", copyNumber.toString) ~>
                    Stream { w =>
                      val bw = new BufferedOutputStream(w)
                      bw.write('[')
                      result.foreach(new Function1[MutationScriptCommandResult, Unit] {
                        var didOne = false
                        def apply(r: MutationScriptCommandResult) {
                          if(didOne) bw.write(',')
                          else didOne = true
                          writeResult(bw, r, tmp)
                        }
                      })
                      bw.write(']')
                      bw.flush()

                      log.debug("Non-linear index seeks: {}", tmp.stats.nonLinearIndexSeeks)
                      log.debug("Non-linear data seeks: {}", tmp.stats.nonLinearDataSeeks)
                    }
                case Left(error) =>
                  error
              }
            case Left(response) =>
              response
          }
        }
        responseBuilder(resp)
      }
    }

    def doDeleteDataset(req: HttpRequest): HttpResponse = {
      deleteDataset(datasetId) match {
        case DatasetDropper.Success =>
          OK ~>
            Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(DateTime.now)) ~>
            Header("X-SODA2-Truth-Copy-Number", 0.toString) ~>
            Header("X-SODA2-Truth-Version", 0.toString) ~>
            Content(JsonContentType, "[]")
        case DatasetDropper.FailureNotFound =>
          notFoundError(formatDatasetId(datasetId))
        case DatasetDropper.FailureWriteLock =>
          writeLockError(datasetId)
      }
    }


    val suffixHashAlg = "SHA1"
    val suffixHashLen = MessageDigest.getInstance(suffixHashAlg).getDigestLength
    def doExportFile(req: HttpRequest): HttpResponse = {
      val servReq = req.servletRequest
      val precondition = req.precondition
      val schemaHash = Option(servReq.getParameter("schemaHash"))
      val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")
      val onlyColumns = Option(servReq.getParameterValues("c")).map(_.flatMap { c =>
        norm(c).split(',').map(new UserColumnId(_)) }).map(UserColumnIdSet(_ : _*))
      val limit = Option(servReq.getParameter("limit")).map { limStr =>
        try {
          limStr.toLong
        } catch {
          case _: NumberFormatException =>
            return BadRequest ~> Content(TextContentType, "Bad limit")
        }
      }
      val offset = Option(servReq.getParameter("offset")).map { offStr =>
        try {
          offStr.toLong
        } catch {
          case _: NumberFormatException =>
            return BadRequest ~> Content(TextContentType, "Bad offset")
        }
      }
      val copy = Option(servReq.getParameter("copy")).getOrElse("latest").toLowerCase match {
        case "latest" => LatestCopy
        case "published" => PublishedCopy
        case "working" => WorkingCopy
        case other =>
          try { Snapshot(other.toInt) }
          catch { case _: NumberFormatException =>
            return BadRequest ~> Content(TextContentType, "Bad copy selector")
          }
      }
      val sorted = Option(servReq.getParameter("sorted")).getOrElse("true").toLowerCase match {
        case "true" =>
          true
        case "false" =>
          if(limit.isDefined || offset.isDefined) {
            return BadRequest ~> Content(TextContentType, "Cannot page through an unsorted export")
          }
          false
        case _ =>
          return BadRequest ~> Content(TextContentType, "Bad sorted selector")
      }
      val suffix = locally {
        val md = MessageDigest.getInstance(suffixHashAlg)
        md.update(formatDatasetId(datasetId).getBytes(UTF_8))
        md.update(schemaHash.toString.getBytes(UTF_8))
        md.update(onlyColumns.toString.getBytes(UTF_8))
        md.update(limit.toString.getBytes(UTF_8))
        md.update(offset.toString.getBytes(UTF_8))
        md.update(copy.toString.getBytes)
        md.update(sorted.toString.getBytes)
        md.digest()
      }
      precondition.filter(_.endsWith(suffix)) match {
        case Right(newPrecond) =>
          val upstreamPrecondition = newPrecond.map(_.dropRight(suffix.length));
          { resp =>
            val found = datasetContents(datasetId, schemaHash, copy, onlyColumns, limit, offset, upstreamPrecondition, ifModifiedSince, sorted) {
              case Left(newSchema) =>
                mismatchedSchema("req.export.mismatched-schema", datasetId, newSchema)(resp)
              case Right((etag, schema, rowIdCol, locale, approxRowCount, rows)) =>
                resp.setContentType("application/json")
                resp.setCharacterEncoding("utf-8")
                ETag(etag.append(suffix))(resp)
                val out = new BufferedWriter(resp.getWriter)
                val jsonWriter = new CompactJsonWriter(out)
                out.write("[{\"approximate_row_count\":")
                out.write(JNumber(approxRowCount).toString)
                out.write("\n ,\"locale\":")
                jsonWriter.write(JString(locale))
                rowIdCol.foreach { rid =>
                  out.write("\n ,\"pk\":")
                  jsonWriter.write(JsonEncode.toJValue(rid))
                }
                out.write("\n ,\"schema\":")
                jsonWriter.write(JsonEncode.toJValue(schema))
                out.write("\n }")
                while(rows.hasNext) {
                  out.write("\n,")
                  jsonWriter.write(JArray(rows.next()))
                }
                out.write("\n]\n")
                out.flush()
            }

            found match {
              case Exporter.Success(_) => // ok good
              case Exporter.NotFound =>
                notFoundError(formatDatasetId(datasetId))
              case Exporter.NotModified(etags) =>
                notModified(etags.map(_.append(suffix)))(resp)
              case Exporter.PreconditionFailedBecauseNoMatch =>
                preconditionFailed(resp)
            }
          }
        case Left(Precondition.FailedBecauseNoMatch) =>
          preconditionFailed
      }
    }
  }

  def notModified(etags: Seq[EntityTag]) = etags.foldLeft(NotModified) { (resp, etag) => resp ~> ETag(etag) }
  val preconditionFailed = err(PreconditionFailed, "req.precondition-failed")

  def jsonifySchema(schemaObj: Schema) = {
    val Schema(hash, schema, pk, locale) = schemaObj
    val jsonSchema = JObject(schema.iterator.map { case (k,v) => k.underlying -> JString(v.name) }.toMap)
    JObject(Map(
      "hash" -> JString(hash),
      "schema" -> jsonSchema,
      "pk" -> JsonEncode.toJValue(pk),
      "locale" -> JString(locale)
    ))
  }

  object VersionResource extends SodaResource {
    override val get: HttpService = (_: HttpRequest) => OK ~> Content(JsonContentType, buildinfo.BuildInfo.toJson)
  }

  implicit object DatasetIdExtractor extends Extractor[DatasetId] {
    def extract(s: String): Option[DatasetId] =
      parseDatasetId(norm(s))
  }

  implicit object RollupNameExtractor extends Extractor[RollupName] {
    def extract(s: String): Option[RollupName] =
      Some(new RollupName(s))
  }

  val router = locally {
    import SimpleRouteContext._
    Routes(
      // "If the thing is parsable as a DatasetId, do something with it, otherwise give a
      // SODA2 not-found response"
      Route("/dataset", NotFoundDatasetResource("")),
      Route("/dataset/{String}", NotFoundDatasetResource),
      Route("/dataset/{DatasetId}", DatasetResource),
      Route("/dataset/{DatasetId}/schema", DatasetSchemaResource),

      Route("/dataset-rollup/{DatasetId}", DatasetRollupResource),

      Route("/secondary-manifest", SecondaryManifestsResource),
      Route("/secondary-manifest/{String}", SecondaryManifestResource),
      Route("/secondary-manifest/{String}/{DatasetId}", DatasetSecondaryStatusResource),
      Route("/secondaries-of-dataset/{DatasetId}", SecondariesOfDatasetResource),

      Route("/version", VersionResource)
    )
  }

  private def handler(req: HttpRequest): HttpResponse = {
    router(req.requestPath) match {
      case Some(result) =>
        result(req)
      case None =>
        NotFound
    }
  }

  private val errorHandlingHandler = new ErrorAdapter(handler) {
    type Tag = String
    def onException(tag: Tag): HttpResponse = {
      InternalServerError ~> Write(JsonContentType) { w =>
        w.write(s"""{"errorCode":"internal","data":{"tag":"$tag"}}""")
      }
    }

    def errorEncountered(ex: Exception): Tag = {
      val uuid = java.util.UUID.randomUUID().toString
      log.error("Unhandled error; tag = " + uuid, ex)
      uuid
    }
  }

  private val logOptions = NewLoggingHandler.defaultOptions.copy(
                     logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))

  private val metricsOptions = MetricsOptions(serviceConfig.metrics)

  def run(port: Int, broker: ServerBroker) {
    for { reporter <- MetricsReporter.managed(metricsOptions) } {
      val server = new SocrataServerJetty(
                     ThreadRenamingHandler(
                       NewLoggingHandler(logOptions)(errorHandlingHandler)),
                     SocrataServerJetty.defaultOptions.
                       withPort(port).
                       withExtraHandlers(List(SocrataHttpSupport.getHandler(metricsOptions))).
                       withPoolOptions(SocrataServerJetty.Pool(serviceConfig.jettyThreadpool)).
                       withBroker(broker))
      server.run()
    }
  }
}
