package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.JNumber
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.Snapshot
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo
import com.socrata.datacoordinator.util.CopyContextResult
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

case class DatasetSnapshotResource(datasetId: DatasetId,
                                   copyNumber: Long,
                                   deleteSnapshotsTo: (DatasetId, Long) => CopyContextResult[UnanchoredCopyInfo],
                                   formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {
  override def delete = { (req: HttpRequest) =>
    deleteSnapshotsTo(datasetId, copyNumber) match {
      case CopyContextResult.NoSuchDataset => notFoundError(datasetId)
      case CopyContextResult.NoSuchCopy => snapshotNotFoundError(datasetId, Snapshot(copyNumber))
      case CopyContextResult.CopyInfo(snapshots) => OK ~> Json(snapshots)
    }
  }
}
