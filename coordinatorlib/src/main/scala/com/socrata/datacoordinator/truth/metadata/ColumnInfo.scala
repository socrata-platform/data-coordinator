package com.socrata.datacoordinator.truth.metadata

trait ColumnInfo {
  def versionInfo: VersionInfo
  def logicalName: String
  def typeName: String
  def isPrimaryKey: Boolean
}
