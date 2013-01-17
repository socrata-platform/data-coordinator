package com.socrata.datacoordinator
package truth.metadata

import com.socrata.datacoordinator.id.ColumnId
import com.rojoma.json.util.AutomaticJsonCodecBuilder

case class ColumnInfo(versionInfo: VersionInfo, systemId: ColumnId, logicalName: String, typeName: String, physicalColumnBaseBase: String, isUserPrimaryKey: Boolean) {
  lazy val physicalColumnBase = physicalColumnBaseBase + "_" + systemId.underlying
}

object ColumnInfo {
  implicit val jCodec = AutomaticJsonCodecBuilder[ColumnInfo]
}
