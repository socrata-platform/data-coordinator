package com.socrata.datacoordinator.resources.collocation

import java.io.IOException

import com.rojoma.json.io.JsonParseException
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.service.collocation.{CollocatorProvider, Coordinator, InstanceNotFound}
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}

@JsonKeyStrategy(Strategy.Underscore)
case class CollocatedDatasetsResult(datasets: Set[DatasetInternalName])

object CollocatedDatasetsResult {
  implicit val codec = AutomaticJsonCodecBuilder[CollocatedDatasetsResult]

  def empty = CollocatedDatasetsResult(Set.empty)
}

case class CollocationManifestsResource(instanceId: Option[String],
                                        coordinator: Coordinator with CollocatorProvider) extends CollocationSodaResource {

  override protected val log = org.slf4j.LoggerFactory.getLogger(classOf[CollocationManifestsResource])

  override def post = getCollocatedDatasets

  private def getCollocatedDatasets(req: HttpRequest): HttpResponse = {
    try {
      JsonUtil.readJson[Set[DatasetInternalName]](req.servletRequest.getReader) match {
        case Right(datasets) if datasets.isEmpty => Json(CollocatedDatasetsResult.empty)
        case Right(datasets) =>
          instanceId match {
            case Some(instance) =>
              coordinator.collocatedDatasetsOnInstance(instance, datasets) match {
                case Right(result) => responseOK(result)
                case Left(InstanceNotFound(_)) => instanceNotFound(instance)
                case Left(_) => InternalServerError
              }
            case None =>
              coordinator.collocator.collocatedDatasets(datasets) match {
                case Right(result) => responseOK(result)
                case Left(_) => InternalServerError
              }
          }
        case Left(decodeError) =>
          log.warn("Unable to decode request: {}", decodeError.english)
          BadRequest
      }
    } catch {
      case e: IOException =>
        log.error("Unexpected error while handling request", e)
        InternalServerError
      case e: JsonParseException =>
        log.warn("Unable to parse request as JSON", e)
        BadRequest
    }
  }
}
