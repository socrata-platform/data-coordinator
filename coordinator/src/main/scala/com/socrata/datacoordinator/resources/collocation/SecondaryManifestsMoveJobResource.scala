package com.socrata.datacoordinator.resources.collocation

import java.util.UUID

import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.service.collocation._
import com.socrata.http.server.responses.InternalServerError
import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryManifestsMoveJobResource(storeGroup: String,
                                             jobId: String,
                                             provider: CoordinatorProvider with MetricProvider) extends CollocationSodaResource {

  override def get = doGetSecondaryMoveJobs

  import provider._

  private def doGetSecondaryMoveJobs(req: HttpRequest): HttpResponse = {
    withJobId(jobId, req) { id =>
      try {
        val result = coordinator.secondaryGroups(storeGroup).map { group =>
          secondaryMoveJobsForStoreGroup(group, id)
        }.reduce(_ + _)

        responseOK(result)
      } catch {
        case StoreGroupNotFound(group) => storeGroupNotFound(group)
      }
    }
  }

  // throws ErrorResult
  private def secondaryMoveJobsForStoreGroup(storeGroup: String, jobId: UUID): CollocationResult = {
    coordinator.secondaryGroupConfigs.get(storeGroup) match {
      case Some(groupConfig) =>
        val moves = metric.collocationGroup.flatMap { instance =>
          val moveJobs = coordinator.secondaryMoveJobs(instance, jobId).fold(throw _, _.moves)

          val datasetCostMap = moveJobs.map(_.datasetId).toSet.map { dataset: DatasetId =>
            val internalName = DatasetInternalName(instance, dataset)
            val cost = metric.datasetMaxCost(storeGroup, internalName).fold(throw _, identity)

            (dataset, cost)
          }.toMap

          moveJobs.filter { moveJob =>
            groupConfig.instances.contains(moveJob.fromStoreId)
          }.map { moveJob =>
            Move(
              datasetInternalName = DatasetInternalName(instance, moveJob.datasetId),
              storeIdFrom = moveJob.fromStoreId,
              storeIdTo = moveJob.toStoreId,
              cost = datasetCostMap(moveJob.datasetId),
              complete = Some(moveJob.moveFromStoreComplete && moveJob.moveToStoreComplete)
            )
          }
        }

        val status = if (moves.forall(_.complete.get)) Completed else InProgress

        CollocationResult(
          id = Some(jobId),
          status = status,
          cost = moves.map(_.cost).fold(Cost.Zero)(_ + _),
          moves = moves.toSeq
        )
      case None => throw StoreGroupNotFound(storeGroup)
    }
  }
}
