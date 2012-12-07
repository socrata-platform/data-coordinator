package com.socrata.datacoordinator.truth.metadata

trait VersionInfo {
  def tableInfo: DatasetInfo
  def lifecycleVersion: Long
  def lifecycleStage: LifecycleStage
}
