package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.DatasetId
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.HttpResponse
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.truth.{LatestCopy, CopySelector}
import com.rojoma.json.io.{CompactJsonWriter, JsonReaderException, JsonReader}
import com.rojoma.json.ast.{JObject, JString, JNumber, JArray}
import com.socrata.datacoordinator.service.mutator.{MutationScriptCommandResult, MutationException, UniversalDDL}
import com.socrata.http.common.util.TooMuchDataWithoutAcknowledgement
import org.joda.time.format.ISODateTimeFormat
import java.io.{OutputStream, BufferedOutputStream}
import java.nio.charset.StandardCharsets
import com.rojoma.json.codec.JsonCodec

class DatasetSchemaResource(getSchema: (DatasetId, CopySelector) => Option[Schema],
                            ddlMutator: UniversalDDL,
                            formatDatasetId: DatasetId => String,
                            commandReadLimit: Long) {
  val errors = new Errors(formatDatasetId)
  val ddlLib = new DDLLib(formatDatasetId, commandReadLimit)
  import ddlLib._

  def schema(datasetId: DatasetId, version: CopySelector)(req: HttpServletRequest): HttpResponse = {
    val result = for {
      schema <- getSchema(datasetId, version)
    } yield {
      OK ~> DataCoordinatorResource.json(DataCoordinatorResource.jsonifySchema(schema))
    }
    result.getOrElse(errors.versionNotFoundError(datasetId, version))
  }

  def ddl(datasetId: DatasetId, version: CopySelector)(req: HttpServletRequest): HttpResponse = {
    if(version != LatestCopy) return errors.versionNotAllowed
    withDDLishScriptResponse {
      fetchScript(req) match {
        case Right(commands) =>
          val (dataVersion, lastUpdated, results) = ddlMutator.ddlScript(datasetId, commands.iterator)
          createResponse(datasetId, dataVersion, lastUpdated, results)
        case Left(err) =>
          err
      }
    }
  }

  case class service(datasetId: DatasetId, version: CopySelector) extends DataCoordinatorResource {
    override def get = schema(datasetId, version)
    override def post = ddl(datasetId, version)
  }
}
