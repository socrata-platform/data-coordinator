package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

case class DatasetSnapshotsResource(datasetId: DatasetId,
                                    getSnapshots: DatasetId => Option[Seq[UnanchoredCopyInfo]],
                                    formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {
  override def get = { (req: HttpRequest) =>
    getSnapshots(datasetId) match {
      case None => notFoundError(datasetId)
      case Some(snapshots) => OK ~> Json(snapshots)
    }
  }
}
