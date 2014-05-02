package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.DatasetId
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

class SecondariesOfDatasetResource(secondariesOfDataset: DatasetId => Map[String, Long]) {
  def doGetSecondariesOfDataset(datasetId: DatasetId)(req: HttpServletRequest): HttpResponse = {
    val ss = secondariesOfDataset(datasetId)
    OK ~> DataCoordinatorResource.json(ss)
  }

  case class service(datasetId: DatasetId) extends DataCoordinatorResource {
    override def get = doGetSecondariesOfDataset(datasetId)
  }
}
