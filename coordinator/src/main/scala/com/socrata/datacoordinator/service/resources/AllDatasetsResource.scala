package com.socrata.datacoordinator.service.resources

import javax.servlet.http.HttpServletRequest
import com.socrata.datacoordinator.service.mutator.UniversalDatasetCreator
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import java.io.BufferedWriter
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.ast.JString

class AllDatasetsResource(creator: UniversalDatasetCreator,
                          formatDatasetId: DatasetId => String,
                          listDatasets: () => Seq[DatasetId],
                          commandReadLimit: Long) {
  val ddlLib = new DDLLib(formatDatasetId, commandReadLimit)
  import ddlLib._

  def doCreate(req: HttpServletRequest): HttpResponse = {
    withDDLishScriptResponse {
      fetchScript(req) match {
        case Right(commands) =>
          val (datasetId, dataVersion, lastUpdated, results) = creator.createScript(commands.iterator)
          createResponse(datasetId, dataVersion, lastUpdated, results)
        case Left(err) =>
          err
      }
    }
  }

  def doListDatasets(req: HttpServletRequest): HttpResponse = {
    val ds = listDatasets()
    OK ~> ContentType("application/json; charset=utf-8") ~> Write { w =>
      val bw = new BufferedWriter(w)
      val jw = new CompactJsonWriter(bw)
      bw.write('[')
      var didOne = false
      for(dsid <- ds) {
        if(didOne) bw.write(',')
        else didOne = true
        jw.write(JString(formatDatasetId(dsid)))
      }
      bw.write(']')
      bw.flush()
    }
  }

  case object service extends DataCoordinatorResource {
    override def get = doListDatasets
    override def put = doCreate
  }
}
