package com.socrata.datacoordinator
package truth.metadata

trait VersionInfo {
  def tableInfo: DatasetInfo
  def systemId: VersionId
  def lifecycleVersion: Long
  def lifecycleStage: LifecycleStage
}
