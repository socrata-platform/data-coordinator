package com.socrata.datacoordinator.service.resources

import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.socrata.http.server.HttpResponse
import com.socrata.datacoordinator.truth.metadata.Schema

trait DataCoordinatorResource extends SimpleResource {

}

object DataCoordinatorResource {
  val ApplicationJson = ContentType("application/json; charset=utf-8")
  def json[T : JsonCodec](jvalue: T) = ApplicationJson ~> Write { w =>
    JsonUtil.writeJson(w, jvalue, pretty = false, buffer = true)
  }

  def jsonifySchema(schemaObj: Schema) = {
    val Schema(hash, schema, pk, locale) = schemaObj
    val jsonSchema = JObject(schema.iterator.map { case (k,v) => k.underlying -> JString(v.name) }.toMap)
    JObject(Map(
      "hash" -> JString(hash),
      "schema" -> jsonSchema,
      "pk" -> JsonCodec.toJValue(pk),
      "locale" -> JString(locale)
    ))
  }
}
