package com.socrata.datacoordinator.service.resources

import com.rojoma.simplearm.util._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

object VersionResource {
  val responseString = for {
    stream <- managed(getClass.getClassLoader.getResourceAsStream("data-coordinator-version.json"))
    source <- managed(scala.io.Source.fromInputStream(stream)(scala.io.Codec.UTF8))
  } yield source.mkString

  val response =
    OK ~> DataCoordinatorResource.ApplicationJson ~> Content(responseString)

  case object service extends DataCoordinatorResource {
    override val get = Function.const(response) _
  }
}
