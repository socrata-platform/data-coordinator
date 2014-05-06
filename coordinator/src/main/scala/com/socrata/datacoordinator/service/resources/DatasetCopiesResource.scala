package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.mutator.UniversalDatasetCopier
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.HttpServletRequest
import com.rojoma.json.ast.JObject

class DatasetCopiesResource(copier: UniversalDatasetCopier,
                            formatDatasetId: DatasetId => String,
                            commandReadLimit: Long) {
  val ddlLib = new DDLLib(formatDatasetId, commandReadLimit)
  import ddlLib._

  def workingCopyOp(datasetId: DatasetId)(req: HttpServletRequest): HttpResponse = {
    withDDLishScriptResponse {
      fetchDatum(req) match {
        case Right(value) =>
          val (version, lastModified) = copier.copyOp(datasetId, value)
          OK ~>
            responseHeaders(version, lastModified) ~>
            DataCoordinatorResource.json(JObject.canonicalEmpty)
        case Left(err) =>
          err
      }
    }
  }

  case class service(datasetId: DatasetId) extends DataCoordinatorResource {
    override def post = workingCopyOp(datasetId)
  }
}
