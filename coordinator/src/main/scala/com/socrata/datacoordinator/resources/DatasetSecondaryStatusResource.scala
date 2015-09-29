package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.ServiceConfig
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class DatasetSecondaryStatusResource(storeIdOpt: Option[String],
                                          datasetId: DatasetId,
                                          secondaries: Set[String],
                                          versionInStore: (String, DatasetId) => Option[Long],
                                          serviceConfig: ServiceConfig,
                                          ensureInSecondary: (String, DatasetId) => Unit,
                                          ensureInSecondaryGroup: (String, DatasetId) => Unit,
                                          formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetSecondaryStatusResource])

  override def get = storeIdOpt match {
      case Some(storeId: String) => r:HttpRequest => doGetDataVersionInSecondary(r)(storeId)
      case None => _: HttpRequest => NotFound
    }

  override def post = storeIdOpt match {
    case Some(storeId: String) => r:HttpRequest => doUpdateVersionInSecondary(r,storeId)
    case None => _: HttpRequest => NotFound

  }

  private def doGetDataVersionInSecondary(req: HttpRequest)(storeId: String): HttpResponse = {
    if(!secondaries(storeId))
      return NotFound

    versionInStore(storeId, datasetId) match {
      case Some(v) => OK ~> Json(Map("version" -> v))
      case None => NotFound
    }
  }

  private def doUpdateVersionInSecondary(req: HttpRequest, storeId: String): HttpResponse = {
    val defaultSecondaryGroups: Set[String] = serviceConfig.secondary.defaultGroups
    val groupRe = "_(.*)_".r
    storeId match {
      case "_DEFAULT_" => defaultSecondaryGroups.foreach(ensureInSecondaryGroup(_, datasetId))
      case groupRe(g) if serviceConfig.secondary.groups.contains(g) => ensureInSecondaryGroup(g, datasetId)
      case secondary if secondaries(storeId) => ensureInSecondary(secondary, datasetId)
      case _ => return NotFound
    }
    OK
  }
}
