package com.socrata.datacoordinator.service.collocation

import com.socrata.datacoordinator.id.DatasetInternalName

trait Metric {
  val collocationGroup: Set[String]
  def datasetMaxCost(storeGroup: String, dataset: DatasetInternalName): Either[ErrorResult, Cost]
}

trait MetricProvider {
  val metric: Metric
}

case class CoordinatedMetric(collocationGroup: Set[String], coordinator: Coordinator) extends Metric {
  override def datasetMaxCost(storeGroup: String, dataset: DatasetInternalName): Either[ErrorResult, Cost] = {
    try {
      val currentInstances = coordinator.secondariesOfDataset(dataset).fold(throw _, _.getOrElse(throw DatasetNotFound(dataset)).secondaries.keySet)

      val maxCost = currentInstances.intersect(coordinator.secondaryGroupConfigs(storeGroup).instances).flatMap { storeId =>
        coordinator.secondaryMetrics(storeId, dataset).fold(throw _, identity).map { metric =>
          Cost(moves = 1, totalSizeBytes = metric.totalSizeBytes)
        }
      }.reduceOption(Cost.max).getOrElse(Cost.Unknown) // TODO: note we way want to replace Unknown here in the future with and error

      Right(maxCost)
    } catch {
      case error: ErrorResult => Left(error)
    }
  }
}


