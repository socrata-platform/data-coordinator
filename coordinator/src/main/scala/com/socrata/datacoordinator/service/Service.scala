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
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, JsonSoQLDataContext, PostgresSoQLDataContext}
import java.util.concurrent.{TimeUnit, Executors}
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth._
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.{DataSourceFromConfig, StandardDatasetMapLimits}
import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.SqlDataReadingContext
import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfo, UnanchoredCopyInfo}
import scala.concurrent.duration.Duration
import scala.Some
import com.socrata.datacoordinator.secondary.{Secondary, NamedSecondary, PlaybackToSecondary, SecondaryLoader}
import com.socrata.datacoordinator.util.collection.DatasetIdMap
import com.socrata.datacoordinator.id.{ColumnId, DatasetId}
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryManifest
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.primary.{WorkingCopyCreator, Publisher}
import java.net.URLDecoder
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport}
import javax.activation.{MimeTypeParseException, MimeType}
import com.socrata.datacoordinator.secondary.NamedSecondary
import scala.Some
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, PostgresUniverse}
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.common.soql.universe.PostgresUniverseCommonSupport
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep

case class Field(name: String, @JsonKey("type") typ: String)
object Field {
  implicit val jCodec = AutomaticJsonCodecBuilder[Field]
}

class Service(processMutation: Iterator[JValue] => Unit,
              datasetContents: (String, Option[Set[String]]) => (Iterator[JObject] => Unit) => Boolean,
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
    val onlyColumns = Option(req.getParameterValues("c")).map(_.flatMap { c => norm(c).toLowerCase /* FIXME: This needs to happen in the dataset context */.split(',') }.toSet)
    val limit = Option(req.getParameter("limit")).map { limStr =>
    }
    val found = datasetContents(norm(id), onlyColumns) { rows =>
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
          case _: DatasetIdInUseByWriterException =>
            Conflict ~> Content("Dataset in use by some other writer; retry later")
          case r: JsonReaderException =>
            BadRequest ~> Content("Malformed JSON : " + r.getMessage)
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

  val timingReport = new LoggedTimingReport(org.slf4j.LoggerFactory.getLogger("timing-report")) with StackedTimingReport

  val config = ConfigFactory.load()
  val serviceConfig = config.getConfig("com.socrata.coordinator-service")
  println(config.root.render)

  val secondaries = SecondaryLoader.load(serviceConfig.getConfig("secondary.configs"), new File(serviceConfig.getString("secondary.path")))

  val (dataSource, copyInForDataSource) = DataSourceFromConfig(serviceConfig)

  val port = serviceConfig.getInt("network.port")

  val executorService = Executors.newCachedThreadPool()
  try {
    val dataContext = new PostgresSoQLDataContext with JsonSoQLDataContext {
      val dataSource = self.dataSource
      val executorService = self.executorService
      def copyIn(conn: Connection, sql: String, input: Reader): Long =
        copyInForDataSource(conn, sql, input)
      def tablespace(s: String) = Some("pg_default")
      val datasetMapLimits = StandardDatasetMapLimits
      val timingReport = self.timingReport
      val datasetMutatorLockTimeout = Duration.Inf
    }

    def datasetsInStore(storeId: String): DatasetIdMap[Long] =
      using(dataSource.getConnection()) { conn =>
        val secondaryManifest = new SqlSecondaryManifest(conn)
        secondaryManifest.datasets(storeId)
      }
    def versionInStore(storeId: String, datasetId: String): Option[Long] =
      using(dataSource.getConnection()) { conn =>
        val secondaryManifest = new SqlSecondaryManifest(conn)
        val mapReader = new PostgresDatasetMapReader(conn, timingReport)
        for {
          systemId <- mapReader.datasetId(datasetId)
          result <- secondaryManifest.readLastDatasetInfo(storeId, systemId)
        } yield result._1
      }
    def updateVersionInStore(storeId: String, datasetId: String): Unit =
      using(dataSource.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val secondaryManifest = new SqlSecondaryManifest(conn)
        val secondary = secondaries(storeId).asInstanceOf[Secondary[dataContext.CV]]
        val pb = new PlaybackToSecondary[dataContext.CT, dataContext.CV](conn, secondaryManifest, dataContext.sqlRepForColumn, timingReport)
        val mapReader = new PostgresDatasetMapReader(conn, timingReport)
        for {
          systemId <- mapReader.datasetId(datasetId)
          datasetInfo <- mapReader.datasetInfo(systemId)
        } yield {
          val delogger = new SqlDelogger(conn, datasetInfo.logTableName, dataContext.newRowLogCodec)
          pb(systemId, NamedSecondary(storeId, secondary), mapReader, delogger)
        }
        conn.commit()
      }
    def secondariesOfDataset(datasetId: String): Option[Map[String, Long]] =
      using(dataSource.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val mapReader = new PostgresDatasetMapReader(conn, timingReport)
        val secondaryManifest = new SqlSecondaryManifest(conn)
        for {
          systemId <- mapReader.datasetId(datasetId)
        } yield secondaryManifest.stores(systemId)
      }

    val commonSupport = new PostgresUniverseCommonSupport(executorService, _ => None, PostgresCopyIn)

    type SoQLUniverse = PostgresUniverse[SoQLType, Any]
    def soqlUniverse(conn: Connection) =
      new SoQLUniverse(conn, commonSupport, timingReport, ":secondary-watcher")

    val universeProvider = new SimpleArm[SoQLUniverse] {
      def flatMap[B](f: SoQLUniverse => B): B = {
        using(dataSource.getConnection()) { conn =>
          conn.setAutoCommit(false)
          val result = f(soqlUniverse(conn))
          conn.commit()
          result
        }
      }
    }

    val mutationCommon = new MutatorCommon[SoQLType, Any] {
      val repFor = dataContext.jsonRepForColumn(_: AbstractColumnInfoLike)

      def physicalColumnBaseBase(logicalColumnName: String, systemColumn: Boolean): String =
        dataContext.physicalColumnBaseBase(logicalColumnName, systemColumn = systemColumn)

      def isLegalLogicalName(identifier: String): Boolean =
        dataContext.isLegalLogicalName(identifier)

      def isSystemColumnName(identifier: String): Boolean =
        dataContext.isSystemColumn(identifier)

      def magicDeleteKey: String = ":delete"

      def systemSchema = dataContext.systemColumns.mapValues(dataContext.typeContext.nameFromType)
      def systemIdColumnName = dataContext.systemIdColumnName
    }

    val mutator = new Mutator(mutationCommon)

    def processMutation(input: Iterator[JValue]) = {
      for(u <- universeProvider) {
        mutator(u, input)
      }
    }

    def exporter(id: String, columns: Option[Set[String]])(f: Iterator[JObject] => Unit): Boolean = {
      val res = for(u <- universeProvider) yield {
        Exporter.export(u, id, columns) { (schema, it) =>
          val jsonSchema = schema.mapValuesStrict(dataContext.jsonRepForColumn)
          f(it.map { row =>
            val res = new scala.collection.mutable.HashMap[String, JValue]
            row.foreach { case (cid, value) =>
              if(jsonSchema.contains(cid)) {
                val rep = jsonSchema(cid)
                val v = rep.toJValue(value)
                if(JNull != v) res(rep.name) = v
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
