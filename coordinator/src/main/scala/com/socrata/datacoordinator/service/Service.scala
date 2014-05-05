package com.socrata.datacoordinator.service

import javax.servlet.http.HttpServletRequest

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.{ServerBroker, HttpResponse, SocrataServerJetty, HttpService}
import com.rojoma.json.io._
import com.ibm.icu.text.Normalizer
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.http.server.routing._
import com.rojoma.json.io.FieldEvent
import com.rojoma.json.io.StringEvent
import com.rojoma.json.io.IdentifierEvent
import com.socrata.http.server.util.ErrorAdapter
import com.socrata.http.server.util.handlers.{ThreadRenamingHandler, LoggingHandler}
import com.socrata.datacoordinator.service.resources.Errors

class Service(allDatasets: HttpService,
              dataset: DatasetId => HttpService,
              datasetSchema: (DatasetId, CopySelector) => HttpService,
              secondaries: HttpService,
              secondariesOfDataset: DatasetId => HttpService,
              secondaryManifest: String => HttpService,
              datasetSecondaryStatus: (String, DatasetId) => HttpService,
              version: HttpService,
              parseDatasetId: String => Option[DatasetId])
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

  implicit object DatasetIdExtractor extends Extractor[DatasetId] {
    def extract(s: String): Option[DatasetId] =
      parseDatasetId(norm(s))
  }

  implicit object CopySelectorExtractor extends Extractor[CopySelector] {
    def extract(s: String): Option[CopySelector] =
      resources.DatasetResource.copySelectorFromString(s)
  }

  val router = locally {
    import SimpleRouteContext._
    Routes(
      Route("/dataset", allDatasets), // PUT to this to create a dataset
      // "If the thing is parsable as a DatasetId, do something with it, otherwise give a
      // SODA2 not-found response"
      Route("/dataset/{String}", { (d: String) => Function.const(Errors.notFoundError(d)) _ }),
      Route("/dataset/{DatasetId}", dataset), // GET this for export; POST this for upsert; DELETE this for delete
      // Route("/dataset/{DatasetId}/copies", DatasetCopiesResource), // GET this for a list of copies; POST this for a copy-op
      Route("/dataset/{DatasetId}/schema", datasetSchema(_ : DatasetId, LatestCopy)), // POST this for DDL
      Route("/dataset/{DatasetId}/schema/{CopySelector}", datasetSchema),

      Route("/secondary-manifest", secondaries),
      Route("/secondary-manifest/{String}", secondaryManifest),
      Route("/secondary-manifest/{String}/{DatasetId}", datasetSecondaryStatus),
      Route("/secondaries-of-dataset/{DatasetId}", secondariesOfDataset),

      Route("/version", version)
    )
  }

  private def handler(req: HttpServletRequest): HttpResponse = {
    router(req.requestPath.map(norm)) match {
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
    val server = new SocrataServerJetty(new ThreadRenamingHandler(new LoggingHandler(errorHandlingHandler)), port = port, broker = broker)
    server.run()
  }
}
