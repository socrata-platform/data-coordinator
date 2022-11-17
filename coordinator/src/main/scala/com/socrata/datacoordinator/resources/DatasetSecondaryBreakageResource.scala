package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.time.ISO8601.encode._
import com.rojoma.json.v3.util.WrapperFieldEncode

import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.service.ServiceConfig
import com.socrata.http.server._
import com.socrata.http.server.routing.HttpMethods
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.secondary.BrokenSecondaryRecord


case class DatasetSecondaryBreakageResource(storeId: Option[String],
                                            datasetId: Option[DatasetId],
                                            allBrokenDatasets: () => Map[String, Map[DatasetId, BrokenSecondaryRecord]],
                                            brokenDatasetsIn: String => Map[DatasetId, BrokenSecondaryRecord],
                                            brokenDataset: (String, DatasetId) => Option[BrokenSecondaryRecord],
                                            unbreakDataset: (String, DatasetId) => Boolean,
                                            acknowledgeBroken: (String, DatasetId) => Boolean,

                                            formatDatasetId: DatasetId => String) extends SodaResource {

  private def text(s: String) = Content("text/plain", s)

  implicit def fCodec = WrapperFieldEncode[DatasetId](formatDatasetId)

  private def jsonifyRecord(bsr: BrokenSecondaryRecord): JValue = {
    json"""{
      brokenAt: ${bsr.brokenAt.toDate},
      brokenAcknowledgedAt: ?${bsr.brokenAcknowledgedAt.map(_.toDate)},
      retryNum: ${bsr.retryNum},
      replayNum: ${bsr.replayNum}
    }"""
  }

  override def get = { (r: HttpRequest) =>
    (storeId, datasetId) match {
      case (Some(storeId), Some(datasetId)) =>
        brokenDataset(storeId, datasetId) match {
          case Some(result) =>
            OK ~> Json(jsonifyRecord(result))
          case None =>
            NotFound ~> text("Dataset not broken or does not exist in the specified store")
        }
      case (Some(storeId), None) =>
        val result = brokenDatasetsIn(storeId)
        OK ~> Json(result.mapValues(jsonifyRecord))
      case (None, Some(datasetId)) =>
        BadRequest ~> text("How did you manage to make a request with a dataset id but no store id?")
      case (None, None) =>
        val result = allBrokenDatasets()
        OK ~> Json(result.mapValues(_.mapValues(jsonifyRecord)))
    }
  }

  override def post = { (r: HttpRequest) =>
    (storeId, datasetId) match {
      case (Some(storeId), Some(datasetId)) =>
        def notFound =
          NotFound ~> text("Dataset does not exist, is not in the specified store, or is not broken")

        r.queryParameter("ack") match {
          case None | Some("false") =>
            if(unbreakDataset(storeId, datasetId)) {
              OK ~> text("Dataset marked as unbroken")
            } else {
              notFound
            }
          case Some("true") =>
            if(acknowledgeBroken(storeId, datasetId)) {
              OK ~> Content("text/plain", "Dataset acknowledged as broken")
            } else {
              notFound
            }
          case _ =>
            BadRequest ~> text("Invalid `ack` parameter")
        }
      case _ =>
        methodNotAllowed(r)
    }
  }

  override def allowedMethods =
    (storeId, datasetId) match {
      case (Some(_), Some(_)) => Set(HttpMethods.GET, HttpMethods.POST)
      case _ => Set(HttpMethods.GET)
    }
}
