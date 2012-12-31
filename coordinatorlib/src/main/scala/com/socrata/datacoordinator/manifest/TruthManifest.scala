package com.socrata.datacoordinator.manifest

import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetMapWriter}

trait TruthManifest {
  def create(dataset: DatasetMapWriter#DatasetInfo)
  def updatePublishedVersion(dataset: DatasetInfo, version: Long)
  def updateLatestVersion(dataset: DatasetInfo, version: Long)
}
