package com.socrata.datacoordinator.resources.collocation

import java.io.IOException
import java.util.UUID

import com.rojoma.json.io.JsonParseException
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import com.socrata.datacoordinator.external.CollocationError
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.secondary.SecondaryMoveJob
import com.socrata.datacoordinator.service.collocation.{DatasetNotFound, ResourceNotFound, StoreGroupNotFound, StoreNotFound}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryMoveJobsResult(moves: Seq[SecondaryMoveJob])

object SecondaryMoveJobsResult {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMoveJobsResult]
}

@JsonKeyStrategy(Strategy.Underscore)
case class SecondaryMoveJobRequest(jobId: UUID, fromStoreId: String, toStoreId: String)

object SecondaryMoveJobRequest {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMoveJobRequest]
}

sealed abstract class InvalidMoveJob

case object StoreNotAcceptingDatasets extends InvalidMoveJob
case object DatasetNotInStore extends InvalidMoveJob

case class SecondaryMoveJobsResource(storeGroup: String,
                                     datasetId: DatasetId,
                                     secondaryMoveJobs: (String, DatasetId) => Either[ResourceNotFound, SecondaryMoveJobsResult],
                                     ensureSecondaryMoveJob: (String, DatasetId, SecondaryMoveJobRequest) => Either[ResourceNotFound, Either[InvalidMoveJob, SecondaryMoveJob]],
                                     datasetInternalName: DatasetId => DatasetInternalName)
  extends CollocationSodaResource {


  override protected val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryMoveJobsResource])

  override def get = doGetSecondaryMoveJobs

  override def post = doMoveDataset

  def doGetSecondaryMoveJobs(req: HttpRequest): HttpResponse = {
    secondaryMoveJobs(storeGroup, datasetId) match {
      case Right(result) => responseOK(result)
      case Left(StoreGroupNotFound(_)) => storeGroupNotFound(storeGroup)
      case Left(DatasetNotFound(_)) => datasetNotFound(datasetInternalName(datasetId))
      case Left(error) =>
        log.error("Unexpected resource not found while doing move for dataset {}: {}", datasetId, error)
        InternalServerError
    }
  }

  def doMoveDataset(req: HttpRequest): HttpResponse = {
    try {
      JsonUtil.readJson[SecondaryMoveJobRequest](req.servletRequest.getReader) match {
        case Right(request) => ensureSecondaryMoveJob(storeGroup, datasetId, request) match {
          case Right(Right(result)) => responseOK(result)
          case Right(Left(StoreNotAcceptingDatasets)) =>
            errorResponse(
              BadRequest,
              CollocationError.STORE_NOT_ACCEPTING_NEW_DATASETS,
              "store" -> JString(request.toStoreId)
            )
          case Right(Left(DatasetNotInStore)) =>
            errorResponse(
              BadRequest,
              CollocationError.DATASET_NOT_FOUND_IN_STORE,
              "store" -> JString(request.fromStoreId)
            )
          case Left(StoreGroupNotFound(_)) => storeGroupNotFound(storeGroup)
          case Left(DatasetNotFound(_)) => datasetNotFound(datasetInternalName(datasetId))
          case Left(StoreNotFound(store)) => storeNotFound(store, BadRequest)
          case Left(error) =>
            log.error("Unexpected resource not found while doing move for dataset {}: {}", datasetId, error)
            InternalServerError
        }
        case Left(decodeError) =>
          log.warn("Unable to decode request: {}", decodeError.english)
          BadRequest ~> Json(JObject.canonicalEmpty)
      }
    } catch {
      case e: IOException =>
        log.error("Unexpected error while handling request", e)
        InternalServerError
      case e: JsonParseException =>
        log.warn("Unable to parse request as JSON", e)
        BadRequest ~> Json(JObject.canonicalEmpty)
    }
  }
}
