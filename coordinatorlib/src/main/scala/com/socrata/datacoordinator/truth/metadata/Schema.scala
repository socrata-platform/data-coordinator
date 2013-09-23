package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import com.rojoma.json.util.{JsonKey, AutomaticJsonCodecBuilder}

case class SchemaField(@JsonKey("c") userColumnId: UserColumnId, @JsonKey("t") typ: String)
object SchemaField {
  implicit val jCodec = AutomaticJsonCodecBuilder[SchemaField]
}

case class Schema(hash: String, schema: UserColumnIdMap[TypeName], pk: UserColumnId, locale: String)
