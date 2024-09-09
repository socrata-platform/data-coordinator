package com.socrata.datacoordinator.resources.collocation

import java.io.IOException
import java.util.UUID

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.JsonParseException
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.external.{BodyRequestError, CollocationError, ParameterRequestError}
import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.resources.BasicSodaResource
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.responses._

abstract class CollocationSodaResource extends BasicSodaResource {

  def instanceNotFound(instance: String, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.INSTANCE_DOES_NOT_EXIST, "instance" -> JString(instance))

  def storeGroupNotFound(storeGroup: String, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.STORE_GROUP_DOES_NOT_EXIST, "store-group" -> JString(storeGroup))

  def storeNotFound(store: String, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.STORE_DOES_NOT_EXIST, "store" -> JString(store))

  def datasetNotFound(datasetInternalName: DatasetInternalName, resp: HttpResponse = NotFound): HttpResponse =
    errorResponse(resp, CollocationError.DATASET_DOES_NOT_EXIST, "dataset" -> JString(datasetInternalName.underlying))

  def collocateLockTimeout(resp: HttpResponse = Conflict): HttpResponse =
    errorResponse(resp, CollocationError.LOCK_TIMEOUT)

  def withTypedParam[T](name: String, req: HttpRequest, defaultValue: T, transformer: String => T)(handleRequest: T => HttpResponse): HttpResponse = {
    val param = Option(req.servletRequest.getParameter(name)).getOrElse(defaultValue.toString)
    try {
      handleRequest(transformer(param))
    } catch {
      case e: IllegalArgumentException =>
        log.warn(s"Unable to parse parameter $name as ${defaultValue.getClass.toGenericString}", e)
        errorResponse(
          BadRequest,
          ParameterRequestError.UNPARSABLE_VALUE,
          "parameter" -> JString(name),
          "type" -> JString(defaultValue.getClass.toGenericString),
          "value" -> JString(param)
        )
    }
  }

  def withBooleanParam(name: String, req: HttpRequest)(handleRequest: Boolean => HttpResponse): HttpResponse = {
    withTypedParam(name, req, false, _.toBoolean){handleRequest}
  }

  def withUUIDParam(name: String, req: HttpRequest)(handleRequest: UUID => HttpResponse): HttpResponse = {
    withTypedParam(name, req, UUID.randomUUID, UUID.fromString){handleRequest}
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

  def withJobId(jobId: String, req: HttpRequest)(handleRequest: UUID => HttpResponse): HttpResponse = {
    val id = try {
      Right(UUID.fromString(jobId))
    } catch {
      case e: IllegalArgumentException =>
        log.warn("Unable to parse job id {} as UUID", jobId)
        Left(errorResponse(BadRequest, CollocationError.INVALID_JOB_ID, "job-id" -> JString(jobId)))
    }

    id.fold(identity, handleRequest)
  }
}
