package com.socrata.datacoordinator.resources.collocation

import java.io.IOException

import com.rojoma.json.v3.ast.{JObject, JString, JValue}
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.io.JsonParseException
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.external.{BodyRequestError, CollocationError, ParameterRequestError}
import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.resources.SodaResource
import com.socrata.datacoordinator.service.ServiceUtil.JsonContentType
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses.Write
import org.slf4j.Logger

abstract class CollocationSodaResource extends SodaResource {

  protected val log: Logger

  def responseOK[T : JsonEncode](content: T): HttpResponse = {
    OK ~> Json(content, pretty = true)
  }

  def errorResponse(codeSetter: HttpResponse, errorCode: String, data: (String, JValue)*): HttpResponse = {
    val response = JObject(Map(
      "errorCode" -> JString(errorCode),
      "data" -> JObject(data.toMap)
    ))

    log.info(response.toString)

    codeSetter ~> Write(JsonContentType) { w => JsonUtil.writeJson(w, response, pretty = true, buffer = true) }
  }

  def instanceNotFound(instance: String, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.INSTANCE_DOES_NOT_EXIST, "instance" -> JString(instance))

  def storeGroupNotFound(storeGroup: String, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.STORE_GROUP_DOES_NOT_EXIST, "store-group" -> JString(storeGroup))

  def storeNotFound(store: String, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.STORE_DOES_NOT_EXIST, "store" -> JString(store))

  def datasetNotFound(datasetInternalName: DatasetInternalName, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.DATASET_DOES_NOT_EXIST, "dataset" -> JString(datasetInternalName.underlying))

  def withBooleanParam(name: String, req: HttpRequest)(handleRequest: Boolean => HttpResponse): HttpResponse = {
    val param = Option(req.servletRequest.getParameter(name)).getOrElse("false")
    try {
      handleRequest(param.toBoolean)
    } catch {
      case e: IllegalArgumentException =>
        log.warn(s"Unable to parse parameter $name as Boolean", e)
        errorResponse(
          BadRequest,
          ParameterRequestError.UNPARSABLE_VALUE,
          "parameter" -> JString(name),
          "type" -> JString("Boolean"),
          "value" -> JString(param)
        )
    }
  }

  def withPostBody[T : JsonDecode](req: HttpRequest)(f: T => HttpResponse): HttpResponse = {
    try {
      JsonUtil.readJson[T](req.servletRequest.getReader) match {
        case Right(body) => f(body)
        case Left(decodeError) =>
          log.warn("Unable to decode request: {}", decodeError.english)
          errorResponse(BadRequest, BodyRequestError.UNPARSABLE, "message" -> JString(decodeError.english))
      }
    } catch {
      case e: IOException =>
        log.error("Unexpected error while handling request", e)
        InternalServerError
      case e: JsonParseException =>
        log.warn("Unable to parse request as JSON", e)
        errorResponse(BadRequest, BodyRequestError.MALFORMED_JSON, "message" -> JString(e.message))
    }
  }
}
