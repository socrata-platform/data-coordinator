package com.socrata.datacoordinator.resources.collocation

import java.io.IOException
import java.util.UUID

import com.rojoma.json.io.JsonParseException
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.common.collocation.{CollocationLockError, CollocationLockTimeout}
import com.socrata.datacoordinator.service.collocation._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryManifestsCollocateResource(storeGroup: String,
                                               coordinator: Coordinator with CollocatorProvider) extends CollocationSodaResource {

  override protected val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryManifestsCollocateResource])

  override def post = doCollocateDatasets

  private def doCollocateDatasets(req: HttpRequest): HttpResponse = {
    val explain = try {
      Option(req.servletRequest.getParameter("explain")).getOrElse("false").toBoolean
    } catch {
      case e: IllegalArgumentException =>
        log.warn("Unable to parse parameter explain as Boolean", e)
        return BadRequest // TODO: some kind of error response body
    }
    try {
      JsonUtil.readJson[CollocationRequest](req.servletRequest.getReader) match {
        case Right(request) =>
          val jobId = UUID.randomUUID()
          try {
            log.info("Beginning collocation request for job {}", jobId)
            coordinator.collocator.beginCollocation()

            val storeGroups = storeGroup match {
              case "_DEFAULT_" => coordinator.collocator.defaultStoreGroups
              case other => Set(other)
            }

            doCollocationJob(jobId, storeGroups, request, explain) match {
              case Right(result) => responseOK(result)
              case Left(StoreGroupNotFound(group)) => storeGroupNotFound(group)
              case Left(DatasetNotFound(dataset)) => datasetNotFound(dataset, BadRequest)
              case Left(_) => InternalServerError
            }
          } catch {
            case _: CollocationLockTimeout => Conflict
            case _: CollocationLockError => InternalServerError // TODO: what more?
            case error: AssertionError =>
              log.error("Failed assertion while collocating datasets; attempting to rollback job...", error)
              coordinator.collocator.rollbackCollocation(jobId)
              InternalServerError
          } finally {
            coordinator.collocator.commitCollocation()
          }
        case Left(decodeError) =>
          log.warn("Unable to decode request: {}", decodeError.english)
          BadRequest // TODO: some kind of error response body
      }
    } catch {
      case e: IOException =>
        log.error("Unexpected error while handling request", e)
        InternalServerError
      case e: JsonParseException =>
        log.warn("Unable to parse request as JSON", e)
        BadRequest
    }
  }

  private def doCollocationJob(jobId: UUID,
                               storeGroups: Set[String],
                               request: CollocationRequest,
                               explain: Boolean): Either[ErrorResult, CollocationResult] = {
    val baseResult = if (explain) CollocationResult.canonicalEmpty else CollocationResult(jobId)
    Right(storeGroups.foldLeft(baseResult) { case (totalResult, group) =>
      val result =
        if (explain) coordinator.collocator.explainCollocation(group, request)
        else coordinator.collocator.executeCollocation(jobId, group, request)

      result match {
        case Right(groupResult) => totalResult + groupResult
        case Left(error) =>
          if (!explain) {
            log.warn("Rolling back collocation moves for job {}", jobId)
            coordinator.collocator.rollbackCollocation(jobId)
          }
          return Left(error)
      }
    })
  }
}
