package com.socrata.datacoordinator.service.resources

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.id.DatasetId
import org.joda.time.DateTime
import com.socrata.datacoordinator.service.mutator.{MutationException, MutationScriptCommandResult}
import com.socrata.http.server.HttpResponse
import org.joda.time.format.ISODateTimeFormat
import java.io.{OutputStream, BufferedOutputStream}
import com.rojoma.json.ast.{JNumber, JArray, JObject, JString}
import java.nio.charset.StandardCharsets
import com.rojoma.json.io.{JsonReaderException, JsonReader, CompactJsonWriter}
import com.rojoma.json.codec.JsonCodec
import javax.servlet.http.HttpServletRequest
import com.socrata.http.common.util.TooMuchDataWithoutAcknowledgement

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

  private def writeResult(o: OutputStream, r: MutationScriptCommandResult) {
    r match {
      case MutationScriptCommandResult.ColumnCreated(id, typname) =>
        o.write(CompactJsonWriter.toString(JObject(Map("id" -> JsonCodec.toJValue(id), "type" -> JString(typname.name)))).getBytes(StandardCharsets.UTF_8))
      case MutationScriptCommandResult.Uninteresting =>
        o.write('{')
        o.write('}')
    }
  }

  def fetchScript(req: HttpServletRequest): Either[HttpResponse, JArray] =
    DatasetResource.jsonStream(req, commandReadLimit).right.flatMap { case (events, _) =>
      JsonReader.fromEvents(events) match {
        case arr: JArray => Right(arr)
        case _ => Left(errors.contentNotJsonArray)
      }
    }


  def createResponse(datasetId: DatasetId, dataVersion: Long, lastModified: DateTime, results: Seq[MutationScriptCommandResult]): HttpResponse =
    OK ~>
      Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastModified)) ~>
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
}
