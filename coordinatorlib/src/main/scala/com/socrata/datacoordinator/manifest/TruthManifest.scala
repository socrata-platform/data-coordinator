package com.socrata.datacoordinator.manifest

import com.socrata.datacoordinator.truth.metadata.DatasetInfo

trait TruthManifest {
  def create(dataset: DatasetInfo)
  def updatePublishedVersion(dataset: DatasetInfo, version: Long)
  def updateLatestVersion(dataset: DatasetInfo, version: Long)
  def latestVersion(dataset: DatasetInfo): Long
}
