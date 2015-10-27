package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.Json
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class SecondariesOfDatasetResource(datasetId: DatasetId,
                                        secondariesOfDataset: DatasetId => Map[String, Long],
                                        formatDatasetId: DatasetId => String)
  extends ErrorHandlingSodaResource(formatDatasetId)  {


  override val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondariesOfDatasetResource])

  override def get = doGetSecondariesOfDataset


  def doGetSecondariesOfDataset(req: HttpRequest): HttpResponse = {
    OK ~> Json(secondariesOfDataset(datasetId))
  }
}
