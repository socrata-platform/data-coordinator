package com.socrata.querycoordinator

import com.socrata.soql.types.SoQLType

import com.socrata.soql.environment.{TypeName, ColumnName}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.rojoma.json.matcher.{PObject, Variable}

case class Schema(hash: String, schema: Map[ColumnName, SoQLType], pk: ColumnName)
object Schema {
  implicit object SchemaCodec extends JsonCodec[Schema] {
    private implicit val schemaProperCodec = new JsonCodec[Map[ColumnName, SoQLType]] {
      def encode(schema: Map[ColumnName, SoQLType]) =
        JObject(schema.map { case (k,v) => k.name -> JString(v.name.name) })
      def decode(x: JValue) =
        JsonCodec[Map[String, String]].decode(x).map { m => m.map { case (k, v) => ColumnName(k) -> SoQLType.typesByName(TypeName(v)) } }
    }

    private val hashVar = Variable[String]()
    private val schemaVar = Variable[Map[ColumnName, SoQLType]]()
    private val pkVar = Variable[String]()
    private val PSchema = PObject(
      "hash" -> hashVar,
      "schema" -> schemaVar,
      "pk" -> pkVar
    )

    def encode(schemaObj: Schema) = {
      val Schema(hash, schema, pk) = schemaObj
      PSchema.generate(hashVar := hash, schemaVar := schema, pkVar := pk.name)
    }

    def decode(x: JValue) = PSchema.matches(x) map { results =>
      Schema(hashVar(results), schemaVar(results), ColumnName(pkVar(results)))
    }
  }
}
