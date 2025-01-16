package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.service.ServiceConfig
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class DatasetSecondaryStatusResource(storeIdOpt: Option[String],
                                          datasetId: DatasetId,
                                          secondaries: Map[String, String],
                                          versionInStore: (String, DatasetId) => Option[Long],
                                          serviceConfig: ServiceConfig,
                                          ensureInSecondary: (String, String, DatasetId) => Boolean,
                                          ensureInSecondaryGroup: (String, DatasetId, Option[DatasetInternalName]) => Boolean,
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
    if(!secondaries.contains(storeId))
      return NotFound

    versionInStore(storeId, datasetId) match {
      case Some(v) => OK ~> Json(Map("version" -> v))
      case None => NotFound
    }
  }

  private def doUpdateVersionInSecondary(req: HttpRequest, storeId: String): HttpResponse = {
    val defaultSecondaryGroups: Set[String] = serviceConfig.secondary.defaultGroups
    val groupRe = "_(.*)_".r
    val secondariesLike = req.queryParameter("secondaries_like").flatMap(DatasetInternalName(_))
    val found = storeId match {
      case "_DEFAULT_" =>
        defaultSecondaryGroups.toVector.map(ensureInSecondaryGroup(_, datasetId, secondariesLike)).forall(identity) // no side effects in forall
      case groupRe(g) if serviceConfig.secondary.groups.contains(g) => ensureInSecondaryGroup(g, datasetId, secondariesLike)
      case secondary if secondaries.contains(storeId) => ensureInSecondary(secondaries(storeId), secondary, datasetId)
      case _ => false
    }
    if (found) OK else NotFound
  }

  private def doDeleteInSecondary(req: HttpRequest, storeId: String): HttpResponse =
    storeId match {
      case secondary if secondaries.contains(storeId) =>
        if (deleteFromSecondary(secondary, datasetId)) OK else NotFound
      case _ => NotFound
    }
}
