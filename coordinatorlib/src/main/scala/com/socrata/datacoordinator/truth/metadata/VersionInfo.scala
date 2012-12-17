package com.socrata.datacoordinator
package truth.metadata

trait VersionInfo {
  def datasetInfo: DatasetInfo
  def systemId: VersionId
  def lifecycleVersion: Long
  def lifecycleStage: LifecycleStage
}
