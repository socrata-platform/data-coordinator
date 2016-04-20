package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

case class SnapshottedResource(getSnapshots: () => Seq[DatasetInfo],
                               formatDatasetId: DatasetId => String) extends SodaResource {
  override def get = { (req: HttpRequest) =>
    OK ~> Json(getSnapshots().map { ds => formatDatasetId(ds.systemId) })
  }
}
