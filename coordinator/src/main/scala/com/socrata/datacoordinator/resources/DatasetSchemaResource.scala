package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class DatasetSchemaResource(datasetId: DatasetId,
                                 getSchema: DatasetId => Option[Schema],
                                 formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetSchemaResource])

  override def get = { (req: HttpRequest) =>
    getSchema(datasetId) match {
      case None => notFoundError(datasetId)
      case Some(schema) => OK ~> Write(JsonContentType) { w => JsonUtil.writeJson(w, jsonifySchema(schema)) }
    }
  }
}
