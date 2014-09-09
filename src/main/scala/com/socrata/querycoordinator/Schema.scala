package com.socrata.querycoordinator

import com.socrata.querycoordinator.util.SoQLTypeCodec
import com.socrata.soql.types.SoQLType

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.JValue
import com.rojoma.json.matcher.{PObject, Variable}

case class Schema(hash: String, schema: Map[String, SoQLType], pk: String)
object Schema {
  implicit object SchemaCodec extends JsonCodec[Schema] {
    private implicit val soQLTypeCodec = SoQLTypeCodec

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
