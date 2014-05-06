package com.socrata.datacoordinator.service.resources

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.id.DatasetId
import org.joda.time.DateTime
import com.socrata.datacoordinator.service.mutator.{MutationException, MutationScriptCommandResult}
import com.socrata.http.server.HttpResponse
import org.joda.time.format.ISODateTimeFormat
import java.io.{BufferedWriter, Writer, OutputStream, BufferedOutputStream}
import com.rojoma.json.ast._
import java.nio.charset.StandardCharsets
import com.rojoma.json.io.{JsonReaderException, JsonReader, CompactJsonWriter}
import com.rojoma.json.codec.JsonCodec
import javax.servlet.http.HttpServletRequest
import com.socrata.http.common.util.TooMuchDataWithoutAcknowledgement
import com.rojoma.json.ast.JString

class DDLLib(formatDatasetId: DatasetId => String, commandReadLimit: Long) {
  val dateTimeFormat = ISODateTimeFormat.dateTime
  val errors = new Errors(formatDatasetId)

  def withDDLishScriptResponse[T](f: => HttpResponse): HttpResponse = {
    try {
      f
    } catch {
      case e: TooMuchDataWithoutAcknowledgement =>
        errors.ddlScriptTooLarge(e.limit)
      case r: JsonReaderException =>
        errors.malformedJson(r)
      case e: MutationException =>
        errors.mutationException(e)
    }
  }

  private def writeResult(w: Writer, r: MutationScriptCommandResult) {
    r match {
      case MutationScriptCommandResult.ColumnCreated(id, typname) =>
        CompactJsonWriter.toWriter(w, JObject(Map("id" -> JsonCodec.toJValue(id), "type" -> JString(typname.name))))
      case MutationScriptCommandResult.Uninteresting =>
        w.write("{}")
    }
  }

  def fetchDatum(req: HttpServletRequest): Either[HttpResponse, JValue] =
    DatasetResource.jsonStream(req, commandReadLimit).right.map { case (events, _) =>
      JsonReader.fromEvents(events)
    }

  def fetchScript(req: HttpServletRequest): Either[HttpResponse, JArray] =
    fetchDatum(req).right.flatMap {
      case arr: JArray => Right(arr)
      case _ => Left(errors.contentNotJsonArray)
    }

  def responseHeaders(dataVersion: Long, lastModified: DateTime): HttpResponse =
    Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastModified)) ~>
      Header("X-SODA2-Truth-Version", dataVersion.toString)

  def createResponse(datasetId: DatasetId, dataVersion: Long, lastModified: DateTime, results: Seq[MutationScriptCommandResult]): HttpResponse =
    OK ~>
      responseHeaders(dataVersion, lastModified) ~>
      DataCoordinatorResource.ApplicationJson ~>
      Write { w =>
        val bw = new BufferedWriter(w)
        bw.write('[')
        CompactJsonWriter.toWriter(bw, JString(formatDatasetId(datasetId)))
        for(r <- results) {
          bw.write(',')
          writeResult(bw, r)
        }
        bw.write(']')
        bw.flush()
      }
}
