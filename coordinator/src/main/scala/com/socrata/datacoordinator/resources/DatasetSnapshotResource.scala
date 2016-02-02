package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

case class DatasetSnapshotResource(datasetId: DatasetId,
                                   copyNumber: Long,
                                   deleteSnapshotsTo: (DatasetId, Long) => Option[UnanchoredCopyInfo],
                                   formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {
  override def delete = { (req: HttpRequest) =>
    deleteSnapshotsTo(datasetId, copyNumber) match {
      case None => notFoundError(datasetId)
      case Some(snapshots) => OK ~> Json(snapshots)
    }
  }
}
