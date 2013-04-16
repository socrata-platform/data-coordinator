package com.socrata.datacoordinator.service

import scala.concurrent.duration._

import java.io._
import javax.servlet.http.HttpServletRequest

import com.socrata.http.server.implicits._
import com.socrata.http.server.{ServerBroker, HttpResponse, SocrataServerJetty, HttpService}
import com.socrata.http.server.responses._
import com.socrata.http.routing.{ExtractingRouter, RouterSet}
import com.rojoma.json.util.{JsonArrayIterator, AutomaticJsonCodecBuilder, JsonKey, JsonUtil}
import com.rojoma.json.io._
import com.rojoma.json.ast._
import com.ibm.icu.text.Normalizer
import com.socrata.datacoordinator.common.soql.{SoQLTypeContext, SoQLRep, SoQLRowLogCodec}
import java.util.concurrent.{TimeUnit, Executors}
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth._
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.{SoQLCommon, StandardObfuscationKeyGenerator, DataSourceFromConfig, StandardDatasetMapLimits}
import java.sql.Connection
import com.rojoma.simplearm.util._
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.secondary.{Secondary, PlaybackToSecondary, SecondaryLoader}
import com.socrata.datacoordinator.util.collection.DatasetIdMap
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryManifest
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import java.net.URLDecoder
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport}
import javax.activation.{MimeTypeParseException, MimeType}
import com.socrata.datacoordinator.secondary.NamedSecondary
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, PostgresUniverse}
import com.rojoma.simplearm.SimpleArm
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.secondary.NamedSecondary
import com.socrata.datacoordinator.truth.Snapshot
import com.rojoma.json.io.TokenIdentifier
import com.rojoma.json.io.TokenString
import com.socrata.datacoordinator.truth.loader.{NoSuchRowToUpdate, NoSuchRowToDelete, NullPrimaryKey, NoPrimaryKey}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.{RetrySleeper, RetryPolicy}
import com.netflix.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceDiscovery}
import com.socrata.http.server.curator.CuratorBroker
import org.apache.log4j.{BasicConfigurator, PropertyConfigurator}

case class Field(name: String, @JsonKey("type") typ: String)
object Field {
  implicit val jCodec = AutomaticJsonCodecBuilder[Field]
}

class Service(processMutation: Iterator[JValue] => Unit,
              datasetContents: (String, CopySelector, Option[Set[ColumnName]], Option[Long], Option[Long]) => (Iterator[JObject] => Unit) => Boolean,
              secondaries: Set[String],
              datasetsInStore: (String) => DatasetIdMap[Long],
              versionInStore: (String, String) => Option[Long],
              updateVersionInStore: (String, String) => Unit,
              secondariesOfDataset: String => Option[Map[String, Long]])
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

  def jsonStream(req: HttpServletRequest): Either[HttpResponse, JsonEventIterator] = {
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
    Right(new JsonEventIterator(new BlockJsonTokenIterator(reader).map(normalizeJson)))
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

  def doMutation()(req: HttpServletRequest): HttpResponse = {
    jsonStream(req) match {
      case Right(events) =>
        try {
          val iterator = try {
            JsonArrayIterator[JValue](events)
          } catch { case _: JsonBadParse =>
            return err(BadRequest, "req.body.not-json-array")
          }

          processMutation(iterator)

          OK
        } catch {
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
                  "expected-state" -> JString(expectedStage.name))
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

  val router = RouterSet(
    ExtractingRouter[HttpService]("POST", "/mutate")(doMutation _),
    ExtractingRouter[HttpService]("GET", "/export/?")(doExportFile _),
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
  BasicConfigurator.configure()

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  val config = ConfigFactory.load()
  val serviceConfig = config.getConfig("com.socrata.coordinator-service")
  println(config.root.render)

  val secondaries = SecondaryLoader.load(serviceConfig.getConfig("secondary.configs"), new File(serviceConfig.getString("secondary.path")))

  val port = serviceConfig.getInt("network.port")

  case class Zookeeper(ensemble: String, sessionTimeout: Duration, connectTimeout: Duration)
  val zk = Zookeeper(
    serviceConfig.getString("zookeeper.ensemble"),
    serviceConfig.getMilliseconds("zookeeper.session-timeout").longValue.millis,
    serviceConfig.getMilliseconds("zookeeper.connect-timeout").longValue.millis
  )
  case class ServiceAdvertisement(basePath: String, name: String, address: String)
  val advertisement = ServiceAdvertisement(
    serviceConfig.getString("service-advertisement.base-path"),
    serviceConfig.getString("service-advertisement.name"),
    serviceConfig.getString("service-advertisement.address")
  )

  val executorService = Executors.newCachedThreadPool()
  try {
    val common = locally {
      val (dataSource, copyInForDataSource) = DataSourceFromConfig(serviceConfig)

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

    def processMutation(input: Iterator[JValue]) = {
      for(u <- common.universe) {
        mutator(u, input)
      }
    }

    def exporter(id: String, copy: CopySelector, columns: Option[Set[ColumnName]], limit: Option[Long], offset: Option[Long])(f: Iterator[JObject] => Unit): Boolean = {
      val res = for(u <- common.universe) yield {
        Exporter.export(u, id, copy, columns, limit, offset) { (copyCtx, it) =>
          val jsonSchema = copyCtx.schema.mapValuesStrict(common.jsonRepFor)
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

    val serv = new Service(processMutation, exporter,
      secondaries.keySet, datasetsInStore, versionInStore, updateVersionInStore,
      secondariesOfDataset)

    for {
      curator <- managed(CuratorFrameworkFactory.newClient(zk.ensemble, zk.sessionTimeout.toMillis.toInt, zk.connectTimeout.toMillis.toInt, new RetryPolicy {
        def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper) =
          true
      }))
      discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[Void]).
        client(curator).
        basePath(advertisement.basePath).
        build())
    } {
      curator.start()
      discovery.start()
      serv.run(port, new CuratorBroker(discovery, advertisement.address, advertisement.name))
    }

    secondaries.values.foreach(_.shutdown())
  } finally {
    executorService.shutdown()
  }
  executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
}
