package com.socrata.datacoordinator.service

import scala.{collection => sc}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import com.socrata.http.server.implicits._
import com.socrata.http.server.{ServerBroker, HttpResponse, SocrataServerJetty, HttpService}
import com.socrata.http.server.responses._
import com.socrata.http.routing.{ExtractingRouter, RouterSet}
import com.rojoma.json.util.{JsonArrayIterator, AutomaticJsonCodecBuilder, JsonKey, JsonUtil}
import com.rojoma.json.io._
import com.rojoma.json.ast._
import com.ibm.icu.text.Normalizer
import java.util.concurrent.{TimeUnit, Executors}
import com.socrata.datacoordinator.truth._
import com.typesafe.config._
import com.socrata.datacoordinator.common.{SoQLCommon, DataSourceFromConfig, StandardDatasetMapLimits}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.{IndexedTempFile, StackedTimingReport, LoggedTimingReport}
import javax.activation.{MimeTypeParseException, MimeType}
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.datacoordinator.truth.loader._
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder
import com.socrata.http.server.curator.CuratorBroker
import org.apache.log4j.PropertyConfigurator
import java.io.{BufferedOutputStream, Reader, UnsupportedEncodingException, BufferedWriter}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.datacoordinator.id.{ColumnId, DatasetId}
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.loader.NoSuchRowToDelete
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.truth.Snapshot
import com.rojoma.json.io.TokenIdentifier
import com.rojoma.json.io.TokenString
import com.socrata.datacoordinator.truth.loader.NoSuchRowToUpdate
import com.socrata.datacoordinator.truth.loader.VersionMismatch
import scala.collection.immutable.NumericRange
import java.nio.charset.StandardCharsets.UTF_8

case class Field(@JsonKey("c") name: String, @JsonKey("t") typ: String)
object Field {
  implicit val jCodec = AutomaticJsonCodecBuilder[Field]
}

case class Schema(hash: String, schema: Map[ColumnName, TypeName], pk: ColumnName)

class Service(processMutation: (DatasetId, Iterator[JValue], IndexedTempFile) => Iterator[NumericRange[Long]],
              processCreation: (Iterator[JValue], IndexedTempFile) => (DatasetId, Iterator[NumericRange[Long]]),
              getSchema: DatasetId => Option[Schema],
              datasetContents: (DatasetId, CopySelector, Option[Set[ColumnName]], Option[Long], Option[Long]) => ((Seq[Field], Option[ColumnName], String, Iterator[Array[JValue]]) => Unit) => Boolean,
              secondaries: Set[String],
              datasetsInStore: (String) => Map[DatasetId, Long],
              versionInStore: (String, DatasetId) => Option[Long],
              ensureInSecondary: (String, DatasetId) => Unit,
              secondariesOfDataset: DatasetId => Map[String, Long],
              datasets: () => Seq[DatasetId],
              deleteDataset: DatasetId => DatasetDropper.Result,
              commandReadLimit: Long,
              formatDatasetId: DatasetId => String,
              parseDatasetId: String => Option[DatasetId],
              tempFileProvider: () => IndexedTempFile)
{
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def norm(s: String) = Normalizer.normalize(s, normalizationMode)

  def normalizeJson(token: JsonEvent): JsonEvent = {
    def position(t: JsonEvent) = { t.position = token.position; t }
    token match {
      case StringEvent(s) =>
        position(StringEvent(norm(s)))
      case FieldEvent(s) =>
        position(FieldEvent(norm(s)))
      case IdentifierEvent(s) =>
        position(IdentifierEvent(norm(s)))
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

  def doExportFile(idRaw: String)(req: HttpServletRequest): HttpResponse = {
    val normalizedId = norm(idRaw)
    val datasetId = parseDatasetId(normalizedId).getOrElse {
      return notFoundError(normalizedId)
    }
    val onlyColumns = Option(req.getParameterValues("c")).map(_.flatMap { c => norm(c).split(',').map(ColumnName) }.toSet)
    val limit = Option(req.getParameter("limit")).map { limStr =>
      try {
        limStr.toLong
      } catch {
        case _: NumberFormatException =>
          return BadRequest ~> Content("Bad limit")
      }
    }
    val offset = Option(req.getParameter("offset")).map { offStr =>
      try {
        offStr.toLong
      } catch {
        case _: NumberFormatException =>
          return BadRequest ~> Content("Bad offset")
      }
    }
    val copy = Option(req.getParameter("copy")).getOrElse("latest").toLowerCase match {
      case "latest" => LatestCopy
      case "published" => PublishedCopy
      case "working" => WorkingCopy
      case other =>
        try { Snapshot(other.toInt) }
        catch { case _: NumberFormatException =>
          return BadRequest ~> Content("Bad copy selector")
        }
    }
    { resp =>
      val found = datasetContents(datasetId, copy, onlyColumns, limit, offset) { (schema, rowId, locale, rows) =>
        resp.setContentType("application/json")
        resp.setCharacterEncoding("utf-8")
        val out = new BufferedWriter(resp.getWriter)
        val jsonWriter = new CompactJsonWriter(out)
        out.write("[{\"locale\":")
        jsonWriter.write(JString(locale))
        rowId.foreach { rid =>
          out.write("\n ,\"row_id\":")
          jsonWriter.write(JString(rid.name))
        }
        out.write("\n ,\"schema\":")
        jsonWriter.write(JsonCodec.toJValue(schema))
        out.write("\n }")
        while(rows.hasNext) {
          out.write("\n,")
          jsonWriter.write(JArray(rows.next()))
        }
        out.write("\n]\n")
        out.flush()
      }
      if(!found) {
        notFoundError(normalizedId)
      }
    }
  }

  def doGetSecondaries()(req: HttpServletRequest): HttpResponse =
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, secondaries.toSeq, buffer = true))

  def doGetSecondaryManifest(storeId: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val ds = datasetsInStore(storeId)
    val dsConverted = ds.foldLeft(Map.empty[String, Long]) { (acc, kv) =>
      acc + (formatDatasetId(kv._1) -> kv._2)
    }
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, dsConverted, buffer = true))
  }

  def doGetDataVersionInSecondary(storeId: String, datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val datasetId = parseDatasetId(datasetIdRaw).getOrElse { return NotFound }
    versionInStore(storeId, datasetId) match {
      case Some(v) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, Map("version" -> v), buffer = true))
      case None =>
        NotFound
    }
  }

  def doUpdateVersionInSecondary(storeId: String, datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val datasetId = parseDatasetId(datasetIdRaw).getOrElse { return NotFound }
    ensureInSecondary(storeId, datasetId)
    OK
  }

  def doGetSecondariesOfDataset(datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    val datasetId = parseDatasetId(datasetIdRaw).getOrElse { return NotFound }
    val ss = secondariesOfDataset(datasetId)
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, ss, buffer = true))
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
    codeSetter ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
      JsonUtil.writeJson(w, response, pretty = true, buffer = true)
    }
  }

  def withMutationScriptResults[T](f: => HttpResponse): HttpResponse = {
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
        def colErr(msg: String, dataset: DatasetId, name: ColumnName, resp: HttpResponse = BadRequest) = {
          import scala.language.reflectiveCalls
          err(resp, msg,
            "dataset" -> JString(formatDatasetId(dataset)),
            "column" -> JString(name.name))
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
            err(Conflict, "req.script.header.mismatched-schema",
              "dataset" -> JString(formatDatasetId(name)),
              "schema" -> jsonifySchema(schema))
          case Mutator.InvalidCommandFieldValue(obj, field, value) =>
            err(BadRequest, "req.script.command.invalid-field",
              "object" -> obj,
              "field" -> JString(field),
              "value" -> value)
          case Mutator.NoSuchDataset(name) =>
            notFoundError(formatDatasetId(name))
          case Mutator.CannotAcquireDatasetWriteLock(name) =>
            writeLockError(name)
          case Mutator.IncorrectLifecycleStage(name, currentStage, expectedStage) =>
            err(Conflict, "update.dataset.invalid-state",
              "dataset" -> JString(formatDatasetId(name)),
              "actual-state" -> JString(currentStage.name),
              "expected-state" -> JArray(expectedStage.toSeq.map(_.name).map(JString)))
          case Mutator.InitialCopyDrop(name) =>
            err(Conflict, "update.dataset.initial-copy-drop",
              "dataset" -> JString(formatDatasetId(name)))
          case Mutator.ColumnAlreadyExists(dataset, name) =>
            colErr("update.column.exists", dataset, name, Conflict)
          case Mutator.IllegalColumnName(columnName) =>
            err(BadRequest, "update.column.illegal-name",
              "name" -> JString(columnName.name))
          case Mutator.NoSuchType(typeName) =>
            err(BadRequest, "update.type.unknown",
              "type" -> JString(typeName.name))
          case Mutator.NoSuchColumn(dataset, name) =>
            colErr("update.column.not-found", dataset, name)
          case Mutator.InvalidSystemColumnOperation(dataset, name, _) =>
            colErr("update.column.system", dataset, name)
          case Mutator.PrimaryKeyAlreadyExists(datasetName, columnName, existingColumn) =>
            err(BadRequest, "update.row-identifier.already-set",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "column" -> JString(columnName.name),
              "existing-column" -> JString(existingColumn.name))
          case Mutator.InvalidTypeForPrimaryKey(datasetName, columnName, typeName) =>
            err(BadRequest, "update.row-identifier.invalid-type",
              "dataset" -> JString(formatDatasetId(datasetName)),
              "column" -> JString(columnName.name),
              "type" -> JString(typeName.name))
          case Mutator.DuplicateValuesInColumn(dataset, name) =>
            colErr("update.row-identifier.duplicate-values", dataset, name)
          case Mutator.NullsInColumn(dataset, name) =>
            colErr("update.row-identifier.null-values", dataset, name)
          case Mutator.NotPrimaryKey(dataset, name) =>
            colErr("update.row-identifier.not-row-identifier", dataset, name)
          case Mutator.InvalidUpsertCommand(value) =>
            err(BadRequest, "update.script.row-data.invalid-value",
              "value" -> value)
          case Mutator.InvalidValue(columnName, typeName, value) =>
            err(BadRequest, "update.row.unparsable-value",
              "column" -> JString(columnName.name),
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
    }
  }

  def doCreation()(req: HttpServletRequest)(resp: HttpServletResponse) {
    using(tempFileProvider()) { tmp =>
      val responseBuilder = withMutationScriptResults {
        jsonStream(req, commandReadLimit) match {
          case Right((events, boundResetter)) =>
            val iteratorOrError = try {
              Right(JsonArrayIterator[JValue](events))
            } catch { case _: JsonBadParse =>
              Left(err(BadRequest, "req.body.not-json-array"))
            }

            iteratorOrError match {
              case Right(iterator) =>
                val (dataset, result) = processCreation(iterator.map { ev => boundResetter(); ev }, tmp)

                OK ~> ContentType("application/json; charset=utf-8") ~> Stream { w =>
                  val bw = new BufferedOutputStream(w)
                  bw.write('[')
                  bw.write(JString(formatDatasetId(dataset)).toString.getBytes(UTF_8))
                  for(jobs <- result) {
                    bw.write(',')
                    bw.write('[')
                    jobs.foreach(new Function1[Long, Unit] {
                      var didOne = false
                      def apply(job: Long) {
                        if(didOne) bw.write(',')
                        else didOne = true
                        tmp.readRecord(job).get.writeTo(bw)
                      }
                    })
                    bw.write(']')
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

  def doMutation(datasetIdRaw: String)(req: HttpServletRequest)(resp: HttpServletResponse) {
    val normalizedId = norm(datasetIdRaw)
    val datasetId = parseDatasetId(normalizedId).getOrElse {
      notFoundError(normalizedId)(resp)
      return
    }
    using(tempFileProvider()) { tmp =>
      val responseBuilder = withMutationScriptResults {
        jsonStream(req, commandReadLimit) match {
          case Right((events, boundResetter)) =>
            val iteratorOrError = try {
              Right(JsonArrayIterator[JValue](events))
            } catch { case _: JsonBadParse =>
              Left(err(BadRequest, "req.body.not-json-array"))
            }

            iteratorOrError match {
              case Right(iterator) =>
                val result = processMutation(datasetId, iterator.map { ev => boundResetter(); ev }, tmp)

                OK ~> ContentType("application/json; charset=utf-8") ~> Stream { w =>
                  val bw = new BufferedOutputStream(w)
                  bw.write('[')
                  result.foreach(new Function1[NumericRange[Long], Unit] {
                    var didOne = false
                    def apply(jobs: NumericRange[Long]) {
                      if(didOne) bw.write(',')
                      else didOne = true
                      bw.write('[')
                      jobs.foreach(new Function1[Long, Unit] {
                        var didOne = false
                        def apply(job: Long) {
                          if(didOne) bw.write(',')
                          else didOne = true
                          tmp.readRecord(job).get.writeTo(bw)
                        }
                      })
                      bw.write(']')
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

  def jsonifySchema(schemaObj: Schema) = {
    val Schema(hash, schema, pk) = schemaObj
    val jsonSchema = JObject(schema.map { case (k,v) => k.name -> JString(v.name) })
    JObject(Map(
      "hash" -> JString(hash),
      "schema" -> jsonSchema,
      "pk" -> JString(pk.name)
    ))
  }

  def doGetSchema(datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    val normalizedId = norm(datasetIdRaw)
    val result = for {
      datasetId <- parseDatasetId(normalizedId)
      schema <- getSchema(datasetId)
    } yield {
      OK ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
        JsonUtil.writeJson(w, jsonifySchema(schema))
      }
    }
    result.getOrElse(notFoundError(normalizedId))
  }

  def doListDatasets()(req: HttpServletRequest): HttpResponse = {
    val ds = datasets()
    OK ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
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

  def doDeleteDataset(datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    val normalizedId = norm(datasetIdRaw)
    val result = for {
      datasetId <- parseDatasetId(normalizedId)
    } yield {
      deleteDataset(datasetId) match {
        case DatasetDropper.Success =>
          OK ~> ContentType("application/json; charset=utf-8") ~> Content("[]")
        case DatasetDropper.FailureNotFound =>
          notFoundError(normalizedId)
        case DatasetDropper.FailureWriteLock =>
          writeLockError(datasetId)
      }
    }
    result.getOrElse(notFoundError(normalizedId))
  }

  val router = RouterSet(
    ExtractingRouter[HttpService]("POST", "/dataset")(doCreation _),
    ExtractingRouter[HttpService]("POST", "/dataset/?")(doMutation _),
    ExtractingRouter[HttpService]("GET", "/dataset/?/schema")(doGetSchema _),
    ExtractingRouter[HttpService]("DELETE", "/dataset/?")(doDeleteDataset _),
    ExtractingRouter[HttpService]("GET", "/dataset/?")(doExportFile _),
    ExtractingRouter[HttpService]("GET", "/datasets")(doListDatasets _),
    ExtractingRouter[HttpService]("GET", "/secondary-manifest")(doGetSecondaries _),
    ExtractingRouter[HttpService]("GET", "/secondary-manifest/?")(doGetSecondaryManifest _),
    ExtractingRouter[HttpService]("GET", "/secondary-manifest/?/?")(doGetDataVersionInSecondary _),
    ExtractingRouter[HttpService]("POST", "/secondary-manifest/?/?")(doUpdateVersionInSecondary _),
    ExtractingRouter[HttpService]("GET", "/secondaries-of-dataset/?")(doGetSecondariesOfDataset _)
  )

  private def handler(req: HttpServletRequest): HttpResponse = {
    router.apply(req.getMethod, req.getPathInfo.split('/').drop(1)) match {
      case Some(result) =>
        result(req)
      case None =>
        NotFound
    }
  }

  private val errorHandlingHandler = new ErrorAdapter(handler) {
    type Tag = String
    def onException(tag: Tag): HttpResponse = {
      InternalServerError ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
        w.write(s"""{"errorCode":"internal","data":{"tag":"$tag"}}""")
      }
    }

    def errorEncountered(ex: Exception): Tag = {
      val uuid = java.util.UUID.randomUUID().toString
      log.error("Unhandled error; tag = " + uuid, ex)
      uuid
    }
  }

  def run(port: Int, broker: ServerBroker) {
    val server = new SocrataServerJetty(errorHandlingHandler, port = port, broker = broker)
    server.run()
  }
}

abstract class ErrorAdapter(service: HttpServletRequest => HttpResponse) extends (HttpServletRequest => HttpResponse) {
  type Tag

  def apply(req: HttpServletRequest): HttpResponse = {
    val response = try {
      service(req)
    } catch {
      case e: Exception =>
        return handleError(_, e)
    }

    (resp: HttpServletResponse) => try {
      response(resp)
    } catch {
      case e: Exception =>
        handleError(resp, e)
    }
  }

  private def handleError(resp: HttpServletResponse, ex: Exception) {
    val tag = errorEncountered(ex)
    if(!resp.isCommitted) {
      resp.reset()
      onException(tag)(resp)
    }
  }

  def errorEncountered(ex: Exception): Tag

  def onException(tag: Tag): HttpResponse
}
