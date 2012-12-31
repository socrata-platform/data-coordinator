package com.socrata.datacoordinator.manifest

import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter

trait TruthManifest {
  def create(dataset: DatasetMapWriter#DatasetInfo)
  def updatePublishedVersion(dataset: DatasetMapWriter#DatasetInfo, version: Long)
  def updateLatestVersion(dataset: DatasetMapWriter#DatasetInfo, version: Long)
}
