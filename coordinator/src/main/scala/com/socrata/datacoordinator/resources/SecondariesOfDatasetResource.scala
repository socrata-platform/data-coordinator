package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.{JObject, Json}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.thirdparty.json.AdditionalJsonCodecs._
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import org.joda.time.DateTime


@JsonKeyStrategy(Strategy.Underscore)
case class SecondariesOfDatasetResult(truthInstance: String,
                                      truthVersion: Long, // TODO: remove this field once soda-fountain no-longer uses it
                                      latestVersion: Long,
                                      publishedVersion: Option[Long],
                                      unpublishedVersion: Option[Long],
                                      secondaries: Map[String, Long],
                                      feedbackSecondaries: Set[String],
                                      groups: Map[String, Set[String]],
                                      brokenSecondaries: Map[String, DateTime]
                                      )

object SecondariesOfDatasetResult {

  implicit val codec = AutomaticJsonCodecBuilder[SecondariesOfDatasetResult]
}

case class SecondariesOfDatasetResource(datasetId: DatasetId,
                                        secondariesOfDataset: DatasetId => Option[SecondariesOfDatasetResult],
                                        formatDatasetId: DatasetId => String)
  extends ErrorHandlingSodaResource(formatDatasetId)  {


  override val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondariesOfDatasetResource])

  override def get = doGetSecondariesOfDataset

  def doGetSecondariesOfDataset(req: HttpRequest): HttpResponse = {
    secondariesOfDataset(datasetId) match {
      case Some(result) => OK ~> Json(result)
      case None => NotFound ~> Json(JObject.canonicalEmpty)
    }
  }
}
