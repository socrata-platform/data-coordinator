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
import com.socrata.datacoordinator.common.soql.{JsonSoQLDataContext, PostgresSoQLDataContext}
import java.util.concurrent.{TimeUnit, Executors}
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth._
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.StandardDatasetMapLimits
import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.DatasetLockContext
import scala.concurrent.duration.Duration
import scala.Some
import com.socrata.datacoordinator.secondary.SecondaryLoader
import com.socrata.datacoordinator.util.collection.DatasetIdMap
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryManifest

case class Field(name: String, @JsonKey("type") typ: String)
object Field {
  implicit val jCodec = AutomaticJsonCodecBuilder[Field]
}

class Service(storeFile: InputStream => String,
              importFile: (String, String, Seq[Field], Option[String]) => Unit,
              updateFile: (String, InputStream) => Option[JObject],
              datasetContents: String => (Iterator[JObject] => Unit) => Boolean,
              secondaries: Set[String],
              datasetsInStore: (String) => DatasetIdMap[Long],
              versionInStore: (String, DatasetId) => Option[Long],
              updateVersionInStore: (String, DatasetId) => Option[Long])
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
    val found = datasetContents(norm(id)) { rows =>
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
      try { new DatasetId(datasetIdRaw.toLong) }
      catch { case _: NumberFormatException => return NotFound }
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
      try { new DatasetId(datasetIdRaw.toLong) }
      catch { case _: NumberFormatException => return NotFound }
    updateVersionInStore(storeId, datasetId) match {
      case Some(v) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, Map("version" -> v), buffer = true))
      case None =>
        NotFound
    }
  }

  val router = RouterSet(
    ExtractingRouter[HttpService]("POST", "/upload")(doUploadFile _),
    ExtractingRouter[HttpService]("POST", "/import/?")(doImportFile _),
    ExtractingRouter[HttpService]("POST", "/update/?")(doUpdateFile _),
    ExtractingRouter[HttpService]("GET", "/export/?")(doExportFile _),
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

  val dataSource = new PGSimpleDataSource
  dataSource.setServerName(serviceConfig.getString("database.host"))
  dataSource.setPortNumber(serviceConfig.getInt("database.port"))
  dataSource.setDatabaseName(serviceConfig.getString("database.database"))
  dataSource.setUser(serviceConfig.getString("database.username"))
  dataSource.setPassword(serviceConfig.getString("database.password"))

  val port = serviceConfig.getInt("network.port")

  val executorService = Executors.newCachedThreadPool()
  try {
    val dataContext: DataReadingContext with DataWritingContext with JsonDataContext = new PostgresSoQLDataContext with JsonSoQLDataContext with DatasetLockContext {
      val dataSource = self.dataSource
      val executorService = self.executorService
      def copyIn(conn: Connection, sql: String, input: Reader): Long =
        conn.asInstanceOf[org.postgresql.PGConnection].getCopyAPI.copyIn(sql, input)
      def tablespace(s: String) = Some("pg_default")
      val datasetMapLimits = StandardDatasetMapLimits
      val datasetLock: DatasetLock = NoopDatasetLock
      val datasetLockTimeout: Duration = Duration.Inf
    }
    val fileStore = new FileStore(new File("/tmp/filestore"))
    val importer = new FileImporter(fileStore.open, ":deleted", dataContext)
    val exporter = new Exporter(dataContext)

    def datasetsInStore(storeId: String): DatasetIdMap[Long] =
      using(dataSource.getConnection()) { conn =>
        val secondary = new SqlSecondaryManifest(conn)
        secondary.datasets(storeId)
      }
    def versionInStore(storeId: String, datasetId: DatasetId): Option[Long] =
      using(dataSource.getConnection()) { conn =>
        val secondary = new SqlSecondaryManifest(conn)
        secondary.readLastDatasetInfo(storeId, datasetId).map(_._1)
      }
    def updateVersionInStore(storeId: String, datasetId: DatasetId): Option[Long] = ???

    val serv = new Service(fileStore.store, importer.importFile, importer.updateFile, exporter.export, secondaries.keySet, datasetsInStore, versionInStore, updateVersionInStore)
    serv.run(port)
  } finally {
    executorService.shutdown()
  }
  executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
}
