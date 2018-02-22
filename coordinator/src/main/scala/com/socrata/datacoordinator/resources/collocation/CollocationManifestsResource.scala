package com.socrata.datacoordinator.resources.collocation

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.datacoordinator.external.CollocationError
import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.service.collocation.{CollocatorProvider, CoordinatorProvider, InstanceNotFound}
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}

@JsonKeyStrategy(Strategy.Underscore)
case class CollocatedDatasetsResult(datasets: Set[DatasetInternalName])

object CollocatedDatasetsResult {
  implicit val codec = AutomaticJsonCodecBuilder[CollocatedDatasetsResult]

  def empty = CollocatedDatasetsResult(Set.empty)
}

case class CollocationManifestsResource(instanceId: Option[String],
                                        datasetId: Option[String],
                                        provider: CoordinatorProvider with CollocatorProvider) extends CollocationSodaResource {

  override def post = getCollocatedDatasets

  override def delete = dropCollocations

  import provider._

  private def getCollocatedDatasets(req: HttpRequest): HttpResponse = {
    withPostBody[Set[DatasetInternalName]](req) { datasets =>
      if (datasets.isEmpty) return Json(CollocatedDatasetsResult.empty)

      instanceId match {
        case Some(instance) =>
          coordinator.collocatedDatasetsOnInstance(instance, datasets) match {
            case Right(result) => responseOK(result)
            case Left(InstanceNotFound(_)) => instanceNotFound(instance)
            case Left(_) => InternalServerError
          }
        case None =>
          collocator.collocatedDatasets(datasets) match {
            case Right(result) => responseOK(result)
            case Left(_) => InternalServerError
          }
      }
    }
  }

  private def dropCollocations(req: HttpRequest): HttpResponse = {
    (instanceId, datasetId.flatMap(DatasetInternalName(_))) match {
      case (Some(instance), Some(dataset)) =>
        coordinator.dropCollocationsOnInstance(instance, dataset) match {
          case None => OK
          case Some(InstanceNotFound(_)) => instanceNotFound(instance)
          case Some(_) => InternalServerError
        }
      case (None, _) => instanceNotFound("")
      case (_, None) => errorResponse(
        BadRequest,
        CollocationError.INVALID_DATASET_INTERNAL_NAME,
        "dataset" -> JString(datasetId.getOrElse(""))
      )
    }
  }
}
