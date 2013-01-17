package com.socrata.datacoordinator
package truth.metadata

import com.socrata.datacoordinator.id.DatasetId
import com.rojoma.json.util.AutomaticJsonCodecBuilder

case class DatasetInfo(systemId: DatasetId, datasetId: String, tableBaseBase: String) {
  lazy val tableBase = tableBaseBase + "_" + systemId.underlying
  lazy val logTableName = tableBase + "_log"
}

object DatasetInfo {
  implicit val jCodec = AutomaticJsonCodecBuilder[DatasetInfo]
}
