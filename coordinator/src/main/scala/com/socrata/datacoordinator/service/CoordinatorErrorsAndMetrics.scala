package com.socrata.datacoordinator.service

import java.io.UnsupportedEncodingException
import javax.activation.{MimeTypeParseException, MimeType}
import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.{FusedBlockJsonEventIterator, JsonEvent}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.http.server.util.EntityTag
import com.socrata.soql.environment.ColumnName
import com.socrata.thirdparty.metrics.Metrics
import com.socrata.datacoordinator.common.util.DatasetIdNormalizer._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.external._
import com.socrata.datacoordinator.util.jsoncodecs._



class CoordinatorErrorsAndMetrics(formatDatasetId: DatasetId => String) extends Metrics {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])


  def columnErrorResponse(msg: String, commandIndex: Long, dataset: DatasetId, colId: UserColumnId, resp: HttpResponse = BadRequest) = {
    import scala.language.reflectiveCalls
    datasetErrorResponse(resp, msg,
      "dataset" -> JString(formatDatasetId(dataset)),
      "column" -> JsonEncode.toJValue(colId),
      "commandIndex" -> JNumber(commandIndex))
  }

  def fieldNameErrorResponse(msg: String, commandIndex: Long, dataset: DatasetId, fieldName: ColumnName, resp: HttpResponse = BadRequest) = {
    datasetErrorResponse(resp, msg,
      "dataset" -> JString(formatDatasetId(dataset)),
      "field_name" -> JsonEncode.toJValue(fieldName),
      "commandIndex" -> JNumber(commandIndex))
  }

  def datasetErrorResponse(codeSetter: HttpResponse, errorCode: String, data: (String, JValue)*): HttpResponse = {
    val response = JObject(Map(
      "errorCode" -> JString(errorCode),
      "data" -> JObject(data.toMap)
    ))

    log.info(response.toString)

    codeSetter ~> Write(JsonContentType) { w => JsonUtil.writeJson(w, response, pretty = true, buffer = true)}
  }

  def datasetBadRequest(errorCode: String, data: (String, JValue)*): HttpResponse = {
    datasetErrorResponse(BadRequest, errorCode, data:_*)
  }

  def contentTypeBadRequest(contentError: String): HttpResponse = {
    val response = JObject(Map(
    "errorCode" -> JString(ContentTypeRequestError.BAD_REQUEST),
    "contentTypeError" -> JString(contentError)
    ))

    log.info(response.toString)

    BadRequest ~> Content(TextContentType, contentError) ~>
      Write(JsonContentType) { w => JsonUtil.writeJson(w, response, pretty = true, buffer = true)}
  }


  def mismatchedSchema(code: String, datasetId: DatasetId, schema: Schema, data: (String, JValue)*): HttpResponse = {
    mismatchedSchema(code, formatDatasetId(datasetId), schema, data:_*)
  }

  def mismatchedDataVersion(code: String, datasetId: DatasetId, version: Long, data: (String, JValue)*): HttpResponse = {
    mismatchedDataVersion(code, formatDatasetId(datasetId), version, data:_*)
  }

  def noSuchColumnLabel(code: String, datasetId: DatasetId, data: (String, JValue)*): HttpResponse = {
    datasetErrorResponse(BadRequest, code,
      "dataset" -> JString(formatDatasetId(datasetId)),
      "data" -> JObject(data.toMap))
  }


  def mismatchedSchema(code: String, name: String, schema: Schema, data: (String, JValue)*): HttpResponse = {
    datasetErrorResponse(Conflict, code,
      "dataset" -> JString(name),
      "schema" -> jsonifySchema(schema),
      "data" -> JObject(data.toMap))
  }

  def mismatchedDataVersion(code: String, name: String, version: Long, data: (String, JValue)*): HttpResponse = {
    datasetErrorResponse(Conflict, code,
      "dataset" -> JString(name),
      "version" -> JNumber(version),
      "data" -> JObject(data.toMap))
  }

  def notFoundError(datasetId: DatasetId, data: (String, JValue)*): HttpResponse = notFoundError(formatDatasetId(datasetId), data:_*)

  def notFoundError(datasetIdString: String, data: (String, JValue)*): HttpResponse = {
    datasetErrorResponse(NotFound, DatasetUpdateError.DOES_NOT_EXIST,
      "dataset" -> JString(datasetIdString),
      "data" -> JObject(data.toMap))
  }

  def snapshotNotFoundError(datasetId: DatasetId, copy: CopySelector, data: (String, JValue)*): HttpResponse = {
    datasetErrorResponse(NotFound, DatasetUpdateError.SNAPSHOT_DOES_NOT_EXIST,
      "dataset" -> JString(formatDatasetId(datasetId)),
      "copy" -> JsonEncode.toJValue(copy),
      "data" -> JObject(data.toMap))
  }

  def writeLockError(datasetId: DatasetId, data: (String, JValue)*) = {
    datasetErrorResponse(Conflict, DatasetUpdateError.TEMP_NOT_WRITABLE,
      "dataset" -> JString(formatDatasetId(datasetId)),
      "data" -> JObject(data.toMap))
  }

  def jsonStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[HttpResponse, (Iterator[JsonEvent], () => Unit)] = {
    val nullableContentType = req.getContentType
    if(nullableContentType == null) return Left(datasetBadRequest(ContentTypeRequestError.MISSING))

    val contentType =
      try {
        new MimeType(nullableContentType)
      } catch {
        case _: MimeTypeParseException => return Left(datasetBadRequest(ContentTypeRequestError.UNPARSABLE,
          "content-type" -> JString(nullableContentType)))
      }

    if(!contentType.`match`("application/json")) {
      return Left(datasetErrorResponse(UnsupportedMediaType, ContentTypeRequestError.NOT_JSON,
        "content-type" -> JString(contentType.toString)))
    }

    val reader =
      try {
        req.getReader
      } catch {
        case _: UnsupportedEncodingException => return Left(datasetErrorResponse(UnsupportedMediaType,
          ContentTypeRequestError.UNKNOWN_CHARSET, "content-type" -> JString(req.getContentType)))
      }
    val boundedReader = new BoundedReader(reader, approximateMaxDatumBound)
    Right((new FusedBlockJsonEventIterator(boundedReader).map(normalizeJson(_)), boundedReader.resetCount _))
  }

  def notModified(etags: Seq[EntityTag]): HttpResponse = {
    log.info("not modified error: %s  ".format(etags.mkString(",")))
    etags.foldLeft[HttpResponse](NotModified) { (resp, etag) => resp ~> ETag(etag) }
  }

}

