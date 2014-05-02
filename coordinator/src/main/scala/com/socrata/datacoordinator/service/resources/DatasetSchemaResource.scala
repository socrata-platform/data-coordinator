package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.DatasetId
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.HttpResponse
import com.socrata.datacoordinator.truth.metadata.Schema

class DatasetSchemaResource(getSchema: DatasetId => Option[Schema],
                            formatDatasetId: DatasetId => String) {
  def schema(datasetId: DatasetId, version: String)(req: HttpServletRequest): HttpResponse = {
    val result = for {
      schema <- getSchema(datasetId)
    } yield {
      OK ~> DataCoordinatorResource.json(DatasetResource.jsonifySchema(schema))
    }
    result.getOrElse(DatasetResource.notFoundError(formatDatasetId(datasetId)))
  }

  case class service(datasetId: DatasetId, version: String) extends DataCoordinatorResource {
    override def get = schema(datasetId, version)
  }
}
