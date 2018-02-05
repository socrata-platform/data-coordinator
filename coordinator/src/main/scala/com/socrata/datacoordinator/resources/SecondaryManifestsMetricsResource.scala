package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.datacoordinator.external.SecondaryMetricsError
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.SecondaryMetric
import com.socrata.datacoordinator.service.collocation.{DatasetNotFound, ResourceNotFound, StoreNotFound}
import com.socrata.http.server.responses.{InternalServerError, NotFound}
import com.socrata.http.server.{HttpRequest, HttpResponse}

@JsonKeyStrategy(Strategy.Underscore)
case class SecondaryMetricsResult(totalSizeBytes: Option[Long])

object SecondaryMetricsResult {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMetricsResult]

  def canonicalEmpty = SecondaryMetricsResult(None)
}

case class SecondaryManifestsMetricsResource(storeId: String,
                                             datasetId: Option[DatasetId],
                                             secondaryMetrics: (String, Option[DatasetId]) => Either[ResourceNotFound, Option[SecondaryMetric]],
                                             formatDatasetId: DatasetId => String) extends BasicSodaResource {

  override protected val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryManifestsMetricsResource])

  override def get = doGetSecondaryMetrics

  private def doGetSecondaryMetrics(req: HttpRequest): HttpResponse = {
    secondaryMetrics(storeId, datasetId) match {
      case Right(result) => responseOK(
        result match {
          case Some(metric) => SecondaryMetricsResult(Some(metric.totalSizeBytes))
          case None => SecondaryMetricsResult.canonicalEmpty
        })
      case Left(StoreNotFound(_)) =>
        errorResponse(
          NotFound,
          SecondaryMetricsError.STORE_DOES_NOT_EXIST,
          "store" -> JString(storeId)
        )
      case Left(DatasetNotFound(dataset)) =>
        errorResponse(
          NotFound,
          SecondaryMetricsError.DATASET_DOES_NOT_EXIST,
          "dataset" -> JString(formatDatasetId(dataset.datasetId))
        )
      case Left(error) =>
        log.error("Unexpected resource not found while getting secondary metrics", error)
        InternalServerError
    }
  }
}
