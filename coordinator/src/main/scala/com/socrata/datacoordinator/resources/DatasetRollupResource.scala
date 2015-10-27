package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.datacoordinator.truth.metadata.RollupInfo
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class DatasetRollupResource(datasetId: DatasetId,
                                 getRollups: DatasetId => Option[Seq[RollupInfo]],
                                 formatDatasetId: DatasetId => String)
  extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetRollupResource])

  override def get = { (req: HttpRequest) =>
    getRollups(datasetId) match {
      case None => notFoundError(datasetId)
      case Some(rollups) => OK ~> Write(JsonContentType) { w =>
        JsonUtil.writeJson(w, rollups.map { r => r.unanchored })
      }}
  }

}