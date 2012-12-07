package com.socrata.datacoordinator.truth.metadata

trait DatasetInfo {
  def datasetId: String
  def tableBase: String
}
