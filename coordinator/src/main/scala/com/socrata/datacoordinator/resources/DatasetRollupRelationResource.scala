package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.external.RollupError.NO_FOUND_RELATIONS
import com.socrata.datacoordinator.id.{DatasetId, RollupName}
import com.socrata.datacoordinator.resources
import com.socrata.datacoordinator.resources.RelationSide.RelationSide
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.datacoordinator.truth.metadata.{RollupDatasetRelation, RollupInfo}
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._


case class DatasetRollupRelationResource(datasetId: DatasetId,
                                         getRelations: (DatasetId) => Option[Set[RollupDatasetRelation]],
                                         side:RelationSide,
                                         formatDatasetId: DatasetId => String)
  extends ErrorHandlingSodaResource(formatDatasetId) {

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetRollupRelationResource])

  override def get = { (req: HttpRequest) =>
    getRelations(datasetId) match{
      case Some(relations) if relations.nonEmpty =>
        OK ~> Write(JsonContentType) { w =>
          JsonUtil.writeJson(w, relations)
        }
      case _ =>datasetErrorResponse(NotFound,NO_FOUND_RELATIONS,"dataset"->JString(formatDatasetId(datasetId)),"side"->JString(side.toString))
    }
  }

}

object RelationSide extends Enumeration {
  type RelationSide = Value

  val From = Value("FROM")
  val To= Value("TO")
}
