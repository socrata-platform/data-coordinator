package com.socrata.datacoordinator.service

import java.io._
import javax.servlet.http.HttpServletRequest

import com.socrata.http.server.implicits._
import com.socrata.http.server.{HttpResponse, SocrataServerJetty, HttpService}
import com.socrata.http.server.responses._
import com.socrata.http.routing.{ExtractingRouter, RouterSet}
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonKey, JsonUtil}
import com.rojoma.json.io.{JsonReaderException, CompactJsonWriter}
import com.rojoma.json.ast.{JNumber, JObject}
import com.ibm.icu.text.Normalizer
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, JsonSoQLDataContext, PostgresSoQLDataContext}
import java.util.concurrent.{TimeUnit, Executors}
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth._
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.{DataSourceFromConfig, StandardDatasetMapLimits}
import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.{SqlDataReadingContext, DatasetLockContext}
import metadata.UnanchoredCopyInfo
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

case class Field(name: String, @JsonKey("type") typ: String)
object Field {
  implicit val jCodec = AutomaticJsonCodecBuilder[Field]
}

class Service(storeFile: InputStream => String,
              importFile: (String, String, Seq[Field], Option[String]) => Unit,
              updateFile: (String, InputStream) => Option[JObject],
              datasetContents: (String, Option[Set[String]]) => (Iterator[JObject] => Unit) => Boolean,
              publish: (String, String) => UnanchoredCopyInfo,
              copy: (String, String, Boolean) => UnanchoredCopyInfo,
              secondaries: Set[String],
              datasetsInStore: (String) => DatasetIdMap[Long],
              versionInStore: (String, String) => Option[Long],
              updateVersionInStore: (String, String) => Unit)
{
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  def norm(s: String) = Normalizer.normalize(s, Normalizer.NFC)

  def doUploadFile()(req: HttpServletRequest): HttpResponse = {
    try {
      val sha = storeFile(req.getInputStream)
      OK ~> Content(sha)
    } catch {
      case e: IOException =>
        log.error("IO exception while processing upload", e)
        InternalServerError
    }
  }

  def doImportFile(id: String)(req: HttpServletRequest): HttpResponse = {
    val file = Option(req.getParameter("file")).map(norm).getOrElse {
      return BadRequest ~> Content("No file")
    }
    val schema = try {
      Option(req.getParameter("schema")).map(norm).flatMap(JsonUtil.parseJson[Seq[Field]]).getOrElse {
        return BadRequest ~> Content("Cannot parse schema as an array of fields")
      }
    } catch {
      case _: JsonReaderException =>
        return BadRequest ~> Content("Cannot parse schema as JSON")
    }
    val primaryKey = try {
      Option(req.getParameter("primaryKey")).filter(_.nonEmpty).map(norm)
    }

    try {
      importFile(norm(id), file, schema, primaryKey)
    } catch {
      case _: FileNotFoundException =>
        return NotFound
      case e: Exception =>
        log.error("Unexpected exception while importing", e)
        return InternalServerError
    }

    OK
  }

  def doUpdateFile(id: String)(req: HttpServletRequest): HttpResponse = {
    updateFile(norm(id), req.getInputStream) match {
      case Some(report) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Content(report.toString)
      case None =>
        NotFound
    }
  }

  def doExportFile(id: String)(req: HttpServletRequest): HttpResponse = { resp =>
    val onlyColumns = Option(req.getParameterValues("c")).map(_.flatMap { c => norm(c).toLowerCase /* FIXME: This needs to happen in the dataset context */.split(',') }.toSet)
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

  def doPublish(id: String)(req: HttpServletRequest): HttpResponse = { resp =>
    publish(norm(id), "blist")
  }

  def doCopy(id: String)(req: HttpServletRequest): HttpResponse = { resp =>
    copy(norm(id), "blist", true)
  }

  val router = RouterSet(
    ExtractingRouter[HttpService]("POST", "/upload")(doUploadFile _),
    ExtractingRouter[HttpService]("POST", "/import/?")(doImportFile _),
    ExtractingRouter[HttpService]("POST", "/update/?")(doUpdateFile _),
    ExtractingRouter[HttpService]("GET", "/export/?")(doExportFile _),
    ExtractingRouter[HttpService]("POST", "/publish/?")(doPublish _),
    ExtractingRouter[HttpService]("POST", "/copy/?")(doCopy _),
    ExtractingRouter[HttpService]("GET", "/secondary-manifest")(doGetSecondaries _),
    ExtractingRouter[HttpService]("GET", "/secondary-manifest/?")(doGetSecondaryManifest _),
    ExtractingRouter[HttpService]("GET", "/secondary-manifest/?/?")(doGetDataVersionInSecondary _),
    ExtractingRouter[HttpService]("POST", "/secondary-manifest/?/?")(doUpdateVersionInSecondary _)
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

  val (dataSource, copyInForDataSource) = DataSourceFromConfig(serviceConfig)

  val port = serviceConfig.getInt("network.port")

  val executorService = Executors.newCachedThreadPool()
  try {
    val dataContext: DataReadingContext with DataWritingContext with JsonDataContext with SqlDataReadingContext = new PostgresSoQLDataContext with JsonSoQLDataContext with DatasetLockContext {
      val dataSource = self.dataSource
      val executorService = self.executorService
      def copyIn(conn: Connection, sql: String, input: Reader): Long =
        copyInForDataSource(conn, sql, input)
      def tablespace(s: String) = Some("pg_default")
      val datasetMapLimits = StandardDatasetMapLimits
      val datasetLock: DatasetLock = NoopDatasetLock
      val datasetLockTimeout: Duration = Duration.Inf
    }

    val fileStore = new FileStore(new File("/tmp/filestore"))
    val importer = new FileImporter(fileStore.open, ":deleted", dataContext)
    val exporter = new Exporter(dataContext)
    val publisher = new Publisher(dataContext.datasetMutator)
    val workingCopyCreator = new WorkingCopyCreator(dataContext.datasetMutator)

    def datasetsInStore(storeId: String): DatasetIdMap[Long] =
      using(dataSource.getConnection()) { conn =>
        val secondaryManifest = new SqlSecondaryManifest(conn)
        secondaryManifest.datasets(storeId)
      }
    def versionInStore(storeId: String, datasetId: String): Option[Long] =
      using(dataSource.getConnection()) { conn =>
        val secondaryManifest = new SqlSecondaryManifest(conn)
        val mapReader = new PostgresDatasetMapReader(conn)
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
        val pb = new PlaybackToSecondary[dataContext.CT, dataContext.CV](conn, secondaryManifest, dataContext.sqlRepForColumn)
        val mapReader = new PostgresDatasetMapReader(conn)
        for {
          systemId <- mapReader.datasetId(datasetId)
          datasetInfo <- mapReader.datasetInfo(systemId)
        } yield {
          val delogger = new SqlDelogger(conn, datasetInfo.logTableName, dataContext.newRowLogCodec)
          pb(systemId, NamedSecondary(storeId, secondary), mapReader, delogger)
        }
        conn.commit()
      }

    val serv = new Service(fileStore.store, importer.importFile, importer.updateFile, exporter.export,
      publisher.publish, workingCopyCreator.copyDataset,
      secondaries.keySet, datasetsInStore, versionInStore, updateVersionInStore)
    serv.run(port)

    secondaries.values.foreach(_.shutdown())
  } finally {
    executorService.shutdown()
  }
  executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
}
