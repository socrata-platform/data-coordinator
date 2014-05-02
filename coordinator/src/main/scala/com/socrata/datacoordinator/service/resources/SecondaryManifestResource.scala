package com.socrata.datacoordinator.service.resources

import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.http.server.HttpResponse

class SecondaryManifestResource(secondaries: Set[String],
                                datasetsInStore: (String) => Map[DatasetId, Long],
                                formatDatasetId: DatasetId => String) {
  def doGetSecondaryManifest(storeId: String)(req: HttpServletRequest): HttpResponse = {
    if(!secondaries(storeId)) return NotFound
    val ds = datasetsInStore(storeId)
    val dsConverted = ds.foldLeft(Map.empty[String, Long]) { (acc, kv) =>
      acc + (formatDatasetId(kv._1) -> kv._2)
    }
    OK ~> DataCoordinatorResource.json(dsConverted)
  }

  case class service(storeId: String) extends DataCoordinatorResource {
    override def get = doGetSecondaryManifest(storeId)
  }
}
