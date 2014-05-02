package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.DatasetId
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

class DatasetSecondaryStatusResource(secondaries: Set[String],
                                     versionInStore: (String, DatasetId) => Option[Long],
                                     ensureInSecondary: (String, DatasetId) => Unit,
                                     ensureInSecondaryGroup: (String, DatasetId) => Unit,
                                     groups: Set[String],
                                     defaultGroups: Set[String]) {
  def doGetDataVersionInSecondary(storeId: String, datasetId: DatasetId)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    versionInStore(storeId, datasetId) match {
      case Some(v) =>
        OK ~> DataCoordinatorResource.json(Map("version" -> v))
      case None =>
        NotFound
    }
  }

  val GroupRe = "_(.*)_".r
  def doUpdateVersionInSecondary(storeId: String, datasetId: DatasetId)(req: HttpServletRequest): HttpResponse = {
    storeId match {
      case "_DEFAULT_" => defaultGroups.foreach(ensureInSecondaryGroup(_, datasetId))
      case GroupRe(g) if groups(g) => ensureInSecondaryGroup(g, datasetId)
      case secondary if secondaries(storeId) => ensureInSecondary(secondary, datasetId)
      case _ => return NotFound
    }
    OK
  }

  case class service(secondary: String, datasetId: DatasetId) extends DataCoordinatorResource {
    override def get = doGetDataVersionInSecondary(secondary, datasetId)
    override def post = doUpdateVersionInSecondary(secondary, datasetId)
  }
}
