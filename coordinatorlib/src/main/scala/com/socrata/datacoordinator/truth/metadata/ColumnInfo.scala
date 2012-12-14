package com.socrata.datacoordinator
package truth.metadata

trait ColumnInfo {
  def versionInfo: VersionInfo
  def systemId: ColumnId
  def logicalName: String
  def typeName: String
  def isPrimaryKey: Boolean
}
