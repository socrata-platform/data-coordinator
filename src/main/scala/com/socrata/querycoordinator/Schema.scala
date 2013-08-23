package com.socrata.querycoordinator

import com.socrata.soql.types.SoQLType

import com.socrata.soql.environment.{TypeName, ColumnName}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.rojoma.json.matcher.{PObject, Variable}

case class Schema(hash: String, schema: Map[String, SoQLType], pk: String)
object Schema {
  implicit object SchemaCodec extends JsonCodec[Schema] {
    private implicit object SoQLTypeCodec extends JsonCodec[SoQLType] {
      def encode(t: SoQLType) = JString(t.name.name)
      def decode(v: JValue) = v match {
        case JString(s) => SoQLType.typesByName.get(TypeName(s))
        case _ => None
      }
    }

    private val hashVar = Variable[String]()
    private val schemaVar = Variable[Map[String, SoQLType]]()
    private val pkVar = Variable[String]()
    private val PSchema = PObject(
      "hash" -> hashVar,
      "schema" -> schemaVar,
      "pk" -> pkVar
    )

    def encode(schemaObj: Schema) = {
      val Schema(hash, schema, pk) = schemaObj
      PSchema.generate(hashVar := hash, schemaVar := schema, pkVar := pk)
    }

    def decode(x: JValue) = PSchema.matches(x) map { results =>
      Schema(hashVar(results), schemaVar(results), pkVar(results))
    }
  }
}
