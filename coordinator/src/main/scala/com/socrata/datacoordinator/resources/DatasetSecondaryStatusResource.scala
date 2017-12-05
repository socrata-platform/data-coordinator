package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.ServiceConfig
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class DatasetSecondaryStatusResource(storeIdOpt: Option[String],
                                          datasetId: DatasetId,
                                          secondaries: Set[String],
                                          fullSecondaries: Set[String],
                                          versionInStore: (String, DatasetId) => Option[Long],
                                          serviceConfig: ServiceConfig,
                                          ensureInSecondary: (String, DatasetId) => Boolean,
                                          ensureInSecondaryGroup: (String, DatasetId) => Boolean,
                                          deleteFromSecondary: (String, DatasetId) => Boolean,
                                          formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetSecondaryStatusResource])

  override def get = storeIdOpt match {
      case Some(storeId: String) => r:HttpRequest => doGetDataVersionInSecondary(r)(storeId)
      case None => _: HttpRequest => NotFound
    }

  override def post = storeIdOpt match {
    case Some(storeId: String) => r:HttpRequest => doUpdateVersionInSecondary(r, storeId)
    case None => _: HttpRequest => NotFound

  }

  override def delete = storeIdOpt match {
    case Some(storeId: String) => r:HttpRequest => doDeleteInSecondary(r, storeId)
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
    val found = storeId match {
      case "_DEFAULT_" => defaultSecondaryGroups.toVector.map(ensureInSecondaryGroup(_, datasetId)).forall(identity) // no side effects in forall
      case groupRe(g) if serviceConfig.secondary.groups.contains(g) => ensureInSecondaryGroup(g, datasetId)
      case secondary if fullSecondaries(storeId) => return Forbidden // don't add datasets to full secondaries
      case secondary if secondaries(storeId) => ensureInSecondary(secondary, datasetId)
      case _ => false
    }
    if (found) OK else NotFound
  }

  private def doDeleteInSecondary(req: HttpRequest, storeId: String): HttpResponse =
    storeId match {
      case secondary if secondaries(storeId) =>
        if (deleteFromSecondary(secondary, datasetId)) OK else NotFound
      case _ => NotFound
    }
}
