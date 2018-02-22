package com.socrata.datacoordinator.resources.collocation

import java.util.UUID

import com.socrata.datacoordinator.common.collocation.CollocationLockTimeout
import com.socrata.datacoordinator.service.collocation._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryManifestsCollocateResource(storeGroup: String,
                                               provider: CoordinatorProvider with CollocatorProvider) extends CollocationSodaResource {

  override def post = doCollocateDatasets

  import provider._

  private def doCollocateDatasets(req: HttpRequest): HttpResponse = {
    withBooleanParam("explain", req) { explain =>
      withUUIDParam("job", req) { jobId =>
        withPostBody[CollocationRequest](req) { request =>
          try {
            log.info("Beginning collocation request for job {}", jobId)
            collocator.beginCollocation()

            val storeGroups = coordinator.secondaryGroups(storeGroup)

            doCollocationJob(jobId, storeGroups, request, explain) match {
              case Right(result) => responseOK(result)
              case Left(StoreGroupNotFound(group)) => storeGroupNotFound(group)
              case Left(DatasetNotFound(dataset)) => datasetNotFound(dataset, BadRequest)
              case Left(_) => InternalServerError
            }
          } catch {
            case _: CollocationLockTimeout => Conflict
          } finally {
            collocator.commitCollocation()
          }
        }
      }
    }
  }

  private def doCollocationJob(jobId: UUID,
                               storeGroups: Set[String],
                               request: CollocationRequest,
                               explain: Boolean): Either[ErrorResult, CollocationResult] = {

    def rollbackCollocationJob(moves: Seq[(Move, Boolean)]): Unit = {
      if (!explain) {
        log.error("Attempting to roll back collocation moves for job {}", jobId)
        collocator.rollbackCollocation(jobId, moves)
      }
    }

    val baseResult = if (explain) CollocationResult.canonicalEmpty else CollocationResult(jobId)
    val (collocationResult, _) = storeGroups.foldLeft((baseResult, Seq.empty[(Move, Boolean)])) { case ((totalResult, movesForRollback), group) =>
      val (result, moves) = try {
        if (explain) (collocator.explainCollocation(group, request), Seq.empty)
        else collocator.initiateCollocation(jobId, group, request)
      } catch {
        case error: AssertionError =>
          log.error("Failed assertion while collocating datasets...", error)
          rollbackCollocationJob(movesForRollback)
          return Left(UnexpectedError("Assertion Failure"))
        case error: CollocationLockTimeout =>
          // never acquired lock... so we should not need any rollback
          throw error
        case error: Exception =>
          log.error("Unexpected exception while collocating datasets...", error)
          rollbackCollocationJob(movesForRollback)
          return Left(UnexpectedError(error.getMessage))
      }

      val totalMovesForRollback = movesForRollback ++ moves

      result match {
        case Right(groupResult) =>
          (totalResult + groupResult, totalMovesForRollback)
        case Left(error) =>
          rollbackCollocationJob(totalMovesForRollback)
          return Left(error)
      }
    }

    if (!explain) collocator.saveCollocation(request)

    Right(collocationResult)
  }
}
