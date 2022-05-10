package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.datacoordinator.truth.metadata.IndexInfo
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class DatasetIndexResource(datasetId: DatasetId,
                                getIndexs: DatasetId => Option[Seq[IndexInfo]],
                                formatDatasetId: DatasetId => String)
  extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetIndexResource])

  override def get = { (req: HttpRequest) =>
    getIndexs(datasetId) match {
      case None => notFoundError(datasetId)
      case Some(indexs) => OK ~> Write(JsonContentType) { w =>
        JsonUtil.writeJson(w, indexs.map { r => r.unanchored })
      }}
  }

}