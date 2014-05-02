package com.socrata.datacoordinator.service.resources

import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

class SecondaryManifestsResource(secondaries: Set[String]) {
  def doGetSecondaries(req: HttpServletRequest): HttpResponse =
    OK ~> DataCoordinatorResource.json(secondaries.toSeq)

  case object service extends DataCoordinatorResource {
    override val get = doGetSecondaries _
  }
}
