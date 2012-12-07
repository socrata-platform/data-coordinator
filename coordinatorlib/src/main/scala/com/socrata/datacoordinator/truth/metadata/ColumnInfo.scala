package com.socrata.datacoordinator.truth.metadata

case class ColumnInfo(versionInfo: VersionInfo, systemId: Long, logicalName: String, typeName: String, physicalColumnBase: String, isPrimaryKey: Boolean)
