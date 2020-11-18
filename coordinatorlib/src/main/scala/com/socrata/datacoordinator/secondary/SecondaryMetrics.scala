package com.socrata.datacoordinator.secondary

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

import com.socrata.datacoordinator.id.DatasetId

case class SecondaryMetric(totalSizeBytes: Long)
object SecondaryMetric extends (Long => SecondaryMetric) {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMetric]
}

trait SecondaryMetrics {
  def storeTotal(storeId: String): SecondaryMetric

  def dataset(storeId: String, datasetId: DatasetId): Option[SecondaryMetric]
  def upsertDataset(storeId: String, datasetId: DatasetId, metric: SecondaryMetric): Unit
  def dropDataset(storeId: String, datasetId: DatasetId): Unit
}
