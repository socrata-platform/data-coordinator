package com.socrata.datacoordinator.service.resources

import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.socrata.http.server.HttpResponse

trait DataCoordinatorResource extends SimpleResource {

}

object DataCoordinatorResource {
  val ApplicationJson = ContentType("application/json; charset=utf-8")
  def json[T : JsonCodec](jvalue: T) = ApplicationJson ~> Write { w =>
    JsonUtil.writeJson(w, jvalue, pretty = false, buffer = true)
  }

  def err(codeSetter: HttpResponse, errorCode: String, data: (String, JValue)*): HttpResponse = {
    val response = JObject(Map(
      "errorCode" -> JString(errorCode),
      "data" -> JObject(data.toMap)
    ))
    codeSetter ~> json(response)
  }
}
