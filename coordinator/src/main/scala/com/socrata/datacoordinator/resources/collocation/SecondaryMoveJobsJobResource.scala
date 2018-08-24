package com.socrata.datacoordinator.resources.collocation

import java.util.UUID

import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryMoveJobsJobResource(jobId: String,
                                        secondaryMoveJobs: UUID => SecondaryMoveJobsResult,
                                        deleteJob: UUID => Unit) extends CollocationSodaResource {

  override def get = doGetSecondaryMoveJobs

  override def delete = doDeleteSecondaryMoveJobs

  def doGetSecondaryMoveJobs(req: HttpRequest): HttpResponse = {
    withJobId(jobId, req) { id =>
      responseOK(secondaryMoveJobs(id))
    }
  }

  def doDeleteSecondaryMoveJobs(req: HttpRequest): HttpResponse = {
    withJobId(jobId, req) { id =>
      responseOK(deleteJob(id))
    }
  }
}
