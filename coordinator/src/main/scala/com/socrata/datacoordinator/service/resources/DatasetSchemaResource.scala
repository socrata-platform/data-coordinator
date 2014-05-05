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
                            formatDatasetId: DatasetId => String) {
  val errors = new Errors(formatDatasetId)

  def schema(datasetId: DatasetId, version: CopySelector)(req: HttpServletRequest): HttpResponse = {
    val result = for {
      schema <- getSchema(datasetId, version)
    } yield {
      OK ~> DataCoordinatorResource.json(DataCoordinatorResource.jsonifySchema(schema))
    }
    result.getOrElse(errors.notFoundError(formatDatasetId(datasetId)))
  }

  def withDDLScriptResponse[T](f: => HttpResponse): HttpResponse = {
    import DataCoordinatorResource.err
    try {
      f
    } catch {
      case e: TooMuchDataWithoutAcknowledgement =>
        return err(RequestEntityTooLarge, "req.body.command-too-large",
          "bytes-without-full-datum" -> JNumber(e.limit))
      case r: JsonReaderException =>
        return err(BadRequest, "req.body.malformed-json",
          "row" -> JNumber(r.row),
          "column" -> JNumber(r.column))
      case e: MutationException =>
        errors.mutationException(e)
    }
  }

  private def writeResult(o: OutputStream, r: MutationScriptCommandResult) {
    r match {
      case MutationScriptCommandResult.ColumnCreated(id, typname) =>
        o.write(CompactJsonWriter.toString(JObject(Map("id" -> JsonCodec.toJValue(id), "type" -> JString(typname.name)))).getBytes(StandardCharsets.UTF_8))
      case MutationScriptCommandResult.Uninteresting =>
        o.write('{')
        o.write('}')
    }
  }

  def ddl(datasetId: DatasetId, version: CopySelector)(req: HttpServletRequest): HttpResponse = {
    if(version != LatestCopy) return errors.versionNotAllowed
    val commandReadLimit = 1024*1024*10L

    withDDLScriptResponse {
      val dateTimeFormat = ISODateTimeFormat.dateTime
      DatasetResource.jsonStream(req, commandReadLimit) match {
        case Right((events, _)) =>
          val commands = JsonReader.fromEvents(events) match {
            case arr: JArray => arr
            case _ => return errors.contentNotJsonArray
          }
          val (dataVersion, lastUpdated, results) = ddlMutator.ddlScript(datasetId, commands.iterator)
          OK ~>
            Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastUpdated)) ~>
            Header("X-SODA2-Truth-Version", dataVersion.toString) ~>
            ContentType("application/json; charset=utf-8") ~>
            Stream { w =>
              val bw = new BufferedOutputStream(w)
              bw.write('[')
              bw.write(JString(formatDatasetId(datasetId)).toString.getBytes(StandardCharsets.UTF_8))
              for(r <- results) {
                bw.write(',')
                writeResult(bw, r)
              }
              bw.write(']')
              bw.flush()
            }

        case Left(error) =>
          error
      }
    }
  }

  case class service(datasetId: DatasetId, version: CopySelector) extends DataCoordinatorResource {
    override def get = schema(datasetId, version)
    override def post = ddl(datasetId, version)
  }
}
