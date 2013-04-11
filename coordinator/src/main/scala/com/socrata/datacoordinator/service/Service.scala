package com.socrata.datacoordinator.service

import java.io._
import javax.servlet.http.HttpServletRequest

import com.socrata.http.server.implicits._
import com.socrata.http.server.{HttpResponse, SocrataServerJetty, HttpService}
import com.socrata.http.server.responses._
import com.socrata.http.routing.{ExtractingRouter, RouterSet}
import com.rojoma.json.util.{JsonArrayIterator, AutomaticJsonCodecBuilder, JsonKey, JsonUtil}
import com.rojoma.json.io._
import com.rojoma.json.ast.{JNull, JValue, JNumber, JObject}
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
      return Left(BadRequest ~> Content("No content-type specified"))
    val contentType =
      try { new MimeType(nullableContentType) }
      catch { case _: MimeTypeParseException =>
        return Left(BadRequest ~> Content("Unparsable content-type"))
      }
    if(!contentType.`match`("application/json")) {
      return Left(UnsupportedMediaType ~> Content("Not application/json"))
    }
    val reader =
      try { req.getReader }
      catch { case _: UnsupportedEncodingException =>
        return Left(UnsupportedMediaType ~> Content("Unknown character encoding"))
      }
    Right(new JsonEventIterator(new BlockJsonTokenIterator(reader).map(normalizeJson)))
  }

  def doMutation()(req: HttpServletRequest): HttpResponse = {
    jsonStream(req) match {
      case Right(events) =>
        try {
          val iterator = try {
            JsonArrayIterator[JValue](events)
          } catch { case _: JsonBadParse =>
            return BadRequest ~> Content("Not a JSON array")
          }

          processMutation(iterator)

          OK
        } catch {
          case r: JsonReaderException =>
            BadRequest ~> Content("Malformed JSON : " + r.getMessage)
          case e: Mutator.MutationException =>
            def err(msg: String) =
              e match {
                case dataError: Mutator.RowDataException =>
                  BadRequest ~> Content(dataError.index + ":" + dataError.subindex + ": " + msg)
                case other =>
                  BadRequest ~> Content(other.index + ": " + msg)
              }
            e match {
              case Mutator.EmptyCommandStream() =>
                err("Empty command stream")
              case Mutator.CommandIsNotAnObject(value) =>
                err("Command is not an object: " + value)
              case Mutator.MissingCommandField(value, field) =>
                err("Missing field " + field + " from object " + value)
              case Mutator.InvalidCommandFieldValue(value, field) =>
                err("Invalid value for field " + field + " at mutation script entry: " + value)
              case Mutator.DatasetAlreadyExists(name) =>
                err("Dataset " + name + " already exists")
              case Mutator.NoSuchDataset(name) =>
                err("No such dataset: " + name)
              case Mutator.CannotAcquireDatasetWriteLock(name) =>
                // TODO: This shouldn't be BadRequest
                err("Cannot acquire write lock for dataset " + name)
              case Mutator.IncorrectLifecycleStage(name, stage) =>
                err("Cannot perform copy DDL while dataset " + name + " is in lifecycle stage " + stage.name)
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

  def run(port: Int) {
    val server = new SocrataServerJetty(handler, port = port)
    server.run()
  }
}

object Service extends App { self =>
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  val config = ConfigFactory.load()
  val serviceConfig = config.getConfig("com.socrata.coordinator-service")
  println(config.root.render)

  val secondaries = SecondaryLoader.load(serviceConfig.getConfig("secondary.configs"), new File(serviceConfig.getString("secondary.path")))

  val port = serviceConfig.getInt("network.port")

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
    serv.run(port)

    secondaries.values.foreach(_.shutdown())
  } finally {
    executorService.shutdown()
  }
  executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
}
