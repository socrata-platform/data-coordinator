package com.socrata.datacoordinator.service.collocation

import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.secondary.SecondaryMetric

trait Metric {
  val collocationGroup: Set[String]
  def datasetMaxCost(storeGroup: String, dataset: DatasetInternalName): Either[ErrorResult, Cost]
  def storeMetrics(storeId: String): Either[ErrorResult, SecondaryMetric]
}

trait MetricProvider {
  val metric: Metric
}

case class CoordinatedMetric(collocationGroup: Set[String],
                             coordinator: Coordinator)(implicit costOrdering: Ordering[Cost]) extends Metric {
  override def datasetMaxCost(storeGroup: String, dataset: DatasetInternalName): Either[ErrorResult, Cost] = {
    try {
      val currentInstances = coordinator.secondariesOfDataset(dataset).fold(throw _, _.getOrElse(throw DatasetNotFound(dataset)).secondaries.keySet)

      val costs = for {
        storeId <- currentInstances.intersect(coordinator.secondaryGroupConfigs(storeGroup).instances.keySet)
        metric <- coordinator.secondaryMetrics(storeId, dataset).fold(throw _, identity)
      } yield {
        Cost(moves = 1, totalSizeBytes = metric.totalSizeBytes)
      }
      // TODO: note we way want to replace Unknown here in the future with an error (See EN-22542)
      val maxCost = costs.reduceOption(costOrdering.max).getOrElse(Cost.Unknown)

      Right(maxCost)
    } catch {
      case error: ErrorResult => Left(error)
    }
  }

  override def storeMetrics(storeId: String): Either[ErrorResult, SecondaryMetric] = {
    try {
      val totalSizeBytes = collocationGroup.toSeq.map { instance =>
        coordinator.secondaryMetrics(storeId, instance).fold(throw _, _.totalSizeBytes)
      }.sum

      Right(SecondaryMetric(totalSizeBytes))
    } catch {
      case error: ErrorResult => Left(error)
    }
  }
}


