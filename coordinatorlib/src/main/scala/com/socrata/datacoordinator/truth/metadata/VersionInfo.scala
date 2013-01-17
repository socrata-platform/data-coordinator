package com.socrata.datacoordinator
package truth.metadata

import com.socrata.datacoordinator.id.VersionId
import com.rojoma.json.util.AutomaticJsonCodecBuilder

case class VersionInfo(datasetInfo: DatasetInfo, systemId: VersionId, lifecycleVersion: Long, lifecycleStage: LifecycleStage) {
  // The systemid is needed to prevent clashes in the following situation:
  //   Working copy created
  //   Working copy dropped (note: this enqueues the table for later dropping)
  //   Working copy created
  // If only the lifecycle version were used, the second working copy creation
  // would try to make a table with the same name as the first, which at that
  // point still exists.
  lazy val dataTableName = datasetInfo.tableBase + "_" + systemId.underlying + "_" + lifecycleVersion
}

object VersionInfo {
  private implicit def lifecycleCodec = LifecycleStage.jCodec
  implicit val jCodec = AutomaticJsonCodecBuilder[VersionInfo]
}
