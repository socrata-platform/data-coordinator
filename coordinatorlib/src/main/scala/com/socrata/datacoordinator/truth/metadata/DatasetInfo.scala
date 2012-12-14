package com.socrata.datacoordinator
package truth.metadata

trait DatasetInfo {
  def systemId: DatasetId
  def datasetId: String
  def tableBase: String
}
