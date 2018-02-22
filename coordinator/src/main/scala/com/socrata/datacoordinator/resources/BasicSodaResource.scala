package com.socrata.datacoordinator.resources

import com.rojoma.json.v3.ast.{JObject, JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.service.ServiceUtil.JsonContentType
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses.{Json, OK, Write}
import com.socrata.http.server.implicits._
import org.slf4j.Logger

abstract class BasicSodaResource extends SodaResource {

  protected val log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

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
}
