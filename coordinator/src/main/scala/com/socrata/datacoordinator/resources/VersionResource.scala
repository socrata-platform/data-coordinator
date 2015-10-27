package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


object VersionResource extends SodaResource {

  override val get: HttpService = (_: HttpRequest) =>
    OK ~> Content(JsonContentType, com.socrata.datacoordinator.BuildInfo.toJson)
}