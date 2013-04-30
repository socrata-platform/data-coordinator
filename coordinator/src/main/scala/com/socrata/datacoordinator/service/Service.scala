package com.socrata.datacoordinator.service

import javax.servlet.http.HttpServletRequest

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
import com.socrata.datacoordinator.secondary.{Secondary, SecondaryLoader}
import com.socrata.datacoordinator.util.collection.DatasetIdMap
import java.net.URLDecoder
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport}
import javax.activation.{MimeTypeParseException, MimeType}
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.datacoordinator.secondary.NamedSecondary
import com.socrata.datacoordinator.truth.Snapshot
import com.socrata.datacoordinator.truth.loader.{NullPrimaryKey, NoPrimaryKey}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder
import com.socrata.http.server.curator.CuratorBroker
import org.apache.log4j.PropertyConfigurator
import java.io.{Reader, UnsupportedEncodingException, BufferedWriter}
import com.socrata.datacoordinator.truth.loader.NoSuchRowToDelete
import com.rojoma.json.io.TokenIdentifier
import com.rojoma.json.io.TokenString
import com.socrata.datacoordinator.truth.loader.NoSuchRowToUpdate
import com.socrata.thirdparty.typesafeconfig.Propertizer

case class Field(name: String, @JsonKey("type") typ: String)
object Field {
  implicit val jCodec = AutomaticJsonCodecBuilder[Field]
}

case class Schema(hash: String, schema: Map[ColumnName, TypeName], pk: ColumnName)

class Service(processMutation: (String, Iterator[JValue]) => Iterator[JsonEvent],
              getSchema: String => Option[Schema],
              datasetContents: (String, CopySelector, Option[Set[ColumnName]], Option[Long], Option[Long]) => (Iterator[JObject] => Unit) => Boolean,
              secondaries: Set[String],
              datasetsInStore: (String) => DatasetIdMap[Long],
              versionInStore: (String, String) => Option[Long],
              updateVersionInStore: (String, String) => Unit,
              secondariesOfDataset: String => Option[Map[String, Long]],
              commandReadLimit: Long)
{
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def norm(s: String) = Normalizer.normalize(s, normalizationMode)

  def normalizeJson(token: JsonToken): JsonToken = {
    def position(t: JsonToken) = { t.position = token.position; t }
    token match {
      case TokenString(s) =>
        position(TokenString(norm(s)))
      case TokenIdentifier(s) =>
        position(TokenIdentifier(norm(s)))
      case other =>
        other
    }
  }

  def doExportFile(id: String)(req: HttpServletRequest): HttpResponse = { resp =>
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
    val found = datasetContents(norm(id), copy, onlyColumns, limit, offset) { rows =>
      resp.setContentType("application/json")
      resp.setCharacterEncoding("utf-8")
      val out = new BufferedWriter(resp.getWriter)
      out.write('[')
      val jsonWriter = new CompactJsonWriter(out)
      var didOne = false
      while(rows.hasNext) {
        if(didOne) out.write(',')
        else didOne = true
        jsonWriter.write(rows.next())
      }
      out.write(']')
      out.flush()
    }
    if(!found)
      NotFound(resp)
  }

  def doGetSecondaries()(req: HttpServletRequest): HttpResponse =
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, secondaries.toSeq, buffer = true))

  def doGetSecondaryManifest(storeId: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val ds = datasetsInStore(storeId)
    val dsConverted = ds.foldLeft(Map.empty[String, Long]) { (acc, kv) =>
      acc + (kv._1.toString -> kv._2)
    }
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, dsConverted, buffer = true))
  }

  def doGetDataVersionInSecondary(storeId: String, datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val datasetId =
      try { URLDecoder.decode(datasetIdRaw, "UTF-8") }
      catch { case _: IllegalArgumentException => return BadRequest }
    versionInStore(storeId, datasetId) match {
      case Some(v) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, Map("version" -> v), buffer = true))
      case None =>
        NotFound
    }
  }

  def doUpdateVersionInSecondary(storeId: String, datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val datasetId =
      try { URLDecoder.decode(datasetIdRaw, "UTF-8") }
      catch { case _: IllegalArgumentException => return BadRequest }
    updateVersionInStore(storeId, datasetId)
    OK
  }

  def doGetSecondariesOfDataset(datasetIdRaw: String)(req: HttpServletRequest): HttpResponse = {
    val datasetId =
      try { URLDecoder.decode(datasetIdRaw, "UTF-8") }
      catch { case _: IllegalArgumentException => return BadRequest }
    secondariesOfDataset(datasetId) match {
      case Some(ss) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, ss, buffer = true))
      case None =>
        NotFound
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

  def jsonStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[HttpResponse, (JsonEventIterator, () => Unit)] = {
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
    Right(new JsonEventIterator(new BlockJsonTokenIterator(boundedReader).map(normalizeJson)), boundedReader.resetCount _)
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

  def doMutation(datasetNameRaw: String)(req: HttpServletRequest): HttpResponse = {
    val datasetName = norm(datasetNameRaw)
    jsonStream(req, commandReadLimit) match {
      case Right((events, boundResetter)) =>
        try {
          val iterator = try {
            JsonArrayIterator[JValue](events)
          } catch { case _: JsonBadParse =>
            return err(BadRequest, "req.body.not-json-array")
          }

          val result = processMutation(datasetName, iterator.map { ev => boundResetter(); ev })

          OK ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
            val bw = new BufferedWriter(w)
            EventTokenIterator(result).foreach { t => bw.write(t.asFragment) }
            bw.flush()
          }
        } catch {
          case e: ReaderExceededBound =>
            return err(RequestEntityTooLarge, "req.body.command-too-large",
              "bytes-without-full-datum" -> JNumber(e.bytesRead))
          case r: JsonReaderException =>
            return err(BadRequest, "req.body.malformed-json",
              "row" -> JNumber(r.row),
              "column" -> JNumber(r.column))
          case e: Mutator.MutationException =>
            type ColErr = { def dataset: String; def name: ColumnName }
            def colErr(msg: String, value: ColErr, resp: HttpResponse = BadRequest) = {
              import scala.language.reflectiveCalls
              err(resp, msg,
                "dataset" -> JString(value.dataset),
                "column" -> JString(value.name.name))
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
                  "dataset" -> JString(name),
                  "schema" -> jsonifySchema(schema))
              case Mutator.InvalidCommandFieldValue(obj, field, value) =>
                err(BadRequest, "req.script.command.invalid-field",
                  "object" -> obj,
                  "field" -> JString(field),
                  "value" -> value)
              case Mutator.DatasetAlreadyExists(name) =>
                err(Conflict, "update.dataset.already-exists",
                  "dataset" -> JString(name))
              case Mutator.NoSuchDataset(name) =>
                err(NotFound, "update.dataset.does-not-exist",
                  "dataset" -> JString(name))
              case Mutator.CannotAcquireDatasetWriteLock(name) =>
                err(Conflict, "update.dataset.temporarily-not-writable",
                  "dataset" -> JString(name))
              case Mutator.IncorrectLifecycleStage(name, currentStage, expectedStage) =>
                err(Conflict, "update.dataset.invalid-state",
                  "dataset" -> JString(name),
                  "actual-state" -> JString(currentStage.name),
                  "expected-state" -> JArray(expectedStage.toSeq.map(_.name).map(JString)))
              case Mutator.InitialCopyDrop(name) =>
                err(Conflict, "update.dataset.initial-copy-drop",
                  "dataset" -> JString(name))
              case e: Mutator.ColumnAlreadyExists =>
                colErr("update.column.exists", e, Conflict)
              case Mutator.IllegalColumnName(columnName) =>
                err(BadRequest, "update.column.illegal-name",
                  "name" -> JString(columnName.name))
              case Mutator.NoSuchType(typeName) =>
                err(BadRequest, "update.type.unknown",
                  "type" -> JString(typeName.name))
              case e: Mutator.NoSuchColumn =>
                colErr("update.column.not-found", e)
              case e: Mutator.InvalidSystemColumnOperation =>
                colErr("update.column.system", e)
              case Mutator.PrimaryKeyAlreadyExists(datasetName, columnName, existingColumn) =>
                err(BadRequest, "update.row-identifier.already-set",
                  "dataset" -> JString(datasetName),
                  "column" -> JString(columnName.name),
                  "existing-column" -> JString(existingColumn.name))
              case Mutator.InvalidTypeForPrimaryKey(datasetName, columnName, typeName) =>
                err(BadRequest, "update.row-identifier.invalid-type",
                  "dataset" -> JString(datasetName),
                  "column" -> JString(columnName.name),
                  "type" -> JString(typeName.name))
              case e: Mutator.DuplicateValuesInColumn =>
                colErr("update.row-identifier.duplicate-values", e)
              case e: Mutator.NullsInColumn =>
                colErr("update.row-identifier.null-values", e)
              case e: Mutator.NotPrimaryKey =>
                colErr("update.row-identifier.not-row-identifier", e)
              case Mutator.InvalidUpsertCommand(value) =>
                err(BadRequest, "update.script.row-data.invalid-value",
                  "value" -> value)
              case Mutator.InvalidValue(columnName, typeName, value) =>
                err(BadRequest, "update.row.unparsable-value",
                  "column" -> JString(columnName.name),
                  "type" -> JString(typeName.name),
                  "value" -> value)
              case Mutator.UpsertError(datasetName, NoPrimaryKey | NullPrimaryKey) =>
                err(BadRequest, "update.row.no-id",
                  "dataset" -> JString(datasetName))
              case Mutator.UpsertError(datasetName, NoSuchRowToDelete(id)) =>
                err(BadRequest, "update.row.no-such-id",
                  "dataset" -> JString(datasetName),
                  "value" -> id)
              case Mutator.UpsertError(datasetName, NoSuchRowToUpdate(id)) =>
                err(BadRequest, "update.row.no-such-id",
                  "dataset" -> JString(datasetName),
                  "value" -> id)
            }
        }
      case Left(response) =>
        response
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
    val datasetId = norm(datasetIdRaw)
    getSchema(datasetId) match {
      case Some(schema) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
          JsonUtil.writeJson(w, jsonifySchema(schema))
        }
      case None =>
        NotFound
    }
  }

  val router = RouterSet(
    ExtractingRouter[HttpService]("POST", "/dataset/?")(doMutation _),
    ExtractingRouter[HttpService]("GET", "/dataset/?/schema")(doGetSchema _),
    // ExtractingRouter[HttpService]("DELETE", "/dataset/?")(doDeleteDataset _),
    ExtractingRouter[HttpService]("GET", "/dataset/?")(doExportFile _),
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

  def run(port: Int, broker: ServerBroker) {
    val server = new SocrataServerJetty(handler, port = port, broker = broker)
    server.run()
  }
}

object Service extends App { self =>
  val serviceConfig = try {
    new ServiceConfig(ConfigFactory.load().getConfig("com.socrata.coordinator-service"))
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", serviceConfig.logProperties))

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  println(serviceConfig.config.root.render)

  val secondaries = SecondaryLoader.load(serviceConfig.secondary.configs, serviceConfig.secondary.path)

  val executorService = Executors.newCachedThreadPool()
  try {
    val common = locally {
      val (dataSource, copyInForDataSource) = DataSourceFromConfig(serviceConfig.dataSource)

      com.rojoma.simplearm.util.using(dataSource.getConnection()) { conn =>
        com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
      }

      new SoQLCommon(
        dataSource,
        copyInForDataSource,
        executorService,
        _ => Some("pg_default"),
        new LoggedTimingReport(org.slf4j.LoggerFactory.getLogger("timing-report")) with StackedTimingReport
      )
    }

    def datasetsInStore(storeId: String): DatasetIdMap[Long] =
      for(u <- common.universe) yield {
        u.secondaryManifest.datasets(storeId)
      }

    def versionInStore(storeId: String, datasetId: String): Option[Long] =
      for(u <- common.universe) yield {
        for {
          systemId <- u.datasetMapReader.datasetId(datasetId)
          result <- u.secondaryManifest.readLastDatasetInfo(storeId, systemId)
        } yield result._1
      }

    def updateVersionInStore(storeId: String, datasetId: String): Unit =
      for(u <- common.universe) {
        val secondary = secondaries(storeId).asInstanceOf[Secondary[u.CT, u.CV]]
        val pb = u.playbackToSecondary
        val mapReader = u.datasetMapReader
        for {
          systemId <- mapReader.datasetId(datasetId)
          datasetInfo <- mapReader.datasetInfo(systemId)
        } yield {
          val delogger = u.delogger(datasetInfo)
          pb(systemId, NamedSecondary(storeId, secondary), mapReader, delogger)
        }
      }
    def secondariesOfDataset(datasetId: String): Option[Map[String, Long]] =
      for(u <- common.universe) yield {
        val mapReader = u.datasetMapReader
        val secondaryManifest = u.secondaryManifest
        for {
          systemId <- mapReader.datasetId(datasetId)
        } yield secondaryManifest.stores(systemId)
      }

    val mutator = new Mutator(common.Mutator)

    def processMutation(datasetId: String, input: Iterator[JValue]) = {
      for(u <- common.universe) yield {
        mutator(u, datasetId, input)
      }
    }

    def exporter(id: String, copy: CopySelector, columns: Option[Set[ColumnName]], limit: Option[Long], offset: Option[Long])(f: Iterator[JObject] => Unit): Boolean = {
      val res = for(u <- common.universe) yield {
        Exporter.export(u, id, copy, columns, limit, offset) { (copyCtx, it) =>
          val jsonReps = common.jsonReps(copyCtx.datasetInfo)
          val jsonSchema = copyCtx.schema.mapValuesStrict(jsonReps)
          f(it.map { row =>
            val res = new scala.collection.mutable.HashMap[String, JValue]
            row.foreach { case (cid, value) =>
              if(jsonSchema.contains(cid)) {
                val rep = jsonSchema(cid)
                val v = rep.toJValue(value)
                if(JNull != v) res(rep.name.name) = v
              }
            }
            JObject(res)
          })
        }
      }
      res.isDefined
    }

    val serv = new Service(processMutation, common.Mutator.schemaFinder.getSchema, exporter,
      secondaries.keySet, datasetsInStore, versionInStore, updateVersionInStore,
      secondariesOfDataset, serviceConfig.commandReadLimit)

    for {
      curator <- managed(CuratorFrameworkFactory.builder.
        connectString(serviceConfig.curator.ensemble).
        sessionTimeoutMs(serviceConfig.curator.sessionTimeout.toMillis.toInt).
        connectionTimeoutMs(serviceConfig.curator.connectTimeout.toMillis.toInt).
        retryPolicy(new retry.BoundedExponentialBackoffRetry(serviceConfig.curator.baseRetryWait.toMillis.toInt,
                                                             serviceConfig.curator.maxRetryWait.toMillis.toInt,
                                                             serviceConfig.curator.maxRetries)).
        namespace(serviceConfig.curator.namespace).
        build())
      discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[Void]).
        client(curator).
        basePath(serviceConfig.advertisement.basePath).
        build())
    } {
      curator.start()
      discovery.start()
      serv.run(serviceConfig.network.port, new CuratorBroker(discovery, serviceConfig.advertisement.address, serviceConfig.advertisement.name + "." + serviceConfig.advertisement.instance))
    }

    secondaries.values.foreach(_.shutdown())
  } finally {
    executorService.shutdown()
  }
  executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
}
