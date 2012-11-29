package com.socrata.datacoordinator.truth.metadata

case class ColumnInfo(VersionInfo: VersionInfo, logicalColumnName: String, typeName: String, physicalColumnBase: String)
