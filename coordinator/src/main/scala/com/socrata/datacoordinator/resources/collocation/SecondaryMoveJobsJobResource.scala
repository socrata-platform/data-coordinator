package com.socrata.datacoordinator.resources.collocation

import java.util.UUID

import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryMoveJobsJobResource(jobId: String,
                                        secondaryMoveJobs: UUID => SecondaryMoveJobsResult) extends CollocationSodaResource {

  override def get = doGetSecondaryMoveJobs

  def doGetSecondaryMoveJobs(req: HttpRequest): HttpResponse = {
    withJobId(jobId, req) { id =>
      responseOK(secondaryMoveJobs(id))
    }
  }
}
