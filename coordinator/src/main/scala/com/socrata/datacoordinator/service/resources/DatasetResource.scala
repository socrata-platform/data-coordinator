package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.Schema
import com.rojoma.json.ast.{JString, JObject}
import com.rojoma.json.codec.JsonCodec
import com.socrata.http.server.responses._

class DatasetResource {
  case class service(datasetId: DatasetId) extends DataCoordinatorResource {
  }
}

object DatasetResource {
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

  def notFoundError(datasetId: String) =
    DataCoordinatorResource.err(NotFound, "update.dataset.does-not-exist",
      "dataset" -> JString(datasetId))
}
