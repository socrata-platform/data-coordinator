package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.id.{StrategyType, UserColumnId}
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import com.rojoma.json.v3.util.{JsonKey, AutomaticJsonCodecBuilder}
import com.socrata.datacoordinator.util.jsoncodecs._

case class CompStratSchemaField(@JsonKey("name") strategyType: StrategyType,
                                @JsonKey("parameters") parameters: JObject,
                                @JsonKey("recompute") recompute: Boolean,
                                @JsonKey("source_columns") sourceColumnIds: Seq[UserColumnId])

object CompStratSchemaField {
  implicit val jCodec = AutomaticJsonCodecBuilder[CompStratSchemaField]

  def convert(csi: ComputationStrategyInfo): CompStratSchemaField = {
    val ComputationStrategyInfo(strategyType, recompute, sourceColumnIds, parameters) = csi
    CompStratSchemaField(strategyType, parameters, recompute, sourceColumnIds)
  }
}

case class SchemaField(@JsonKey("c") userColumnId: UserColumnId,
                       @JsonKey("f") fieldName: Option[ColumnName],
                       @JsonKey("s") computationStrategy: Option[CompStratSchemaField],
                       @JsonKey("t") typ: String)
object SchemaField {
  implicit val jCodec = AutomaticJsonCodecBuilder[SchemaField]
}

case class Schema(hash: String, schema: UserColumnIdMap[TypeName], pk: UserColumnId, locale: String)
