package com.socrata.datacoordinator.truth.metadata

case class ColumnInfo(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String)
