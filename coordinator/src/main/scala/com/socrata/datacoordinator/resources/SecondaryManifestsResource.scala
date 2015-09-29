package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.http.server.{HttpResponse, HttpRequest}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


case class SecondaryManifestsResource(storeId: Option[String],
                                      secondaries: Set[String],
                                      datasetsInStore: (String) => Map[DatasetId, Long],
                                      formatDatasetId: DatasetId => String)
  extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryManifestsResource])

  override def get =  {
    storeId match {
      case None => doGetSecondaries
      case Some(id) => doGetSecondaryManifest(_: HttpRequest)(id)
    }
  }

  private def doGetSecondaries(req: HttpRequest): HttpResponse = {
    OK ~> Json(secondaries.toSeq)
  }

  private def doGetSecondaryManifest(req: HttpRequest)(id: String): HttpResponse = {
    if(!secondaries(id))
      return NotFound

    OK ~> Json(datasetsInStore(id).foldLeft(Map.empty[String, Long]) { (acc, kv) =>
      acc + (formatDatasetId(kv._1) -> kv._2) })
  }





}

