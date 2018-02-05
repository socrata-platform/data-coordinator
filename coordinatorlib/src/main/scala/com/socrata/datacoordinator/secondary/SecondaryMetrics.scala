package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.DatasetId

case class SecondaryMetric(totalSizeBytes: Long)

trait SecondaryMetrics {
  def storeTotal(storeId: String): SecondaryMetric

  def dataset(storeId: String, datasetId: DatasetId): Option[SecondaryMetric]
  def upsertDataset(storeId: String, datasetId: DatasetId, metric: SecondaryMetric): Unit
  def dropDataset(storeId: String, datasetId: DatasetId): Unit
}
