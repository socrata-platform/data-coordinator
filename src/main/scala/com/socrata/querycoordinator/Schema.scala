package com.socrata.querycoordinator

import com.socrata.querycoordinator.util.SoQLTypeCodec
import com.socrata.soql.types.SoQLType

import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.matcher.{PObject, Variable}

case class Schema(hash: String, schema: Map[String, SoQLType], pk: String)
object Schema {
  implicit object SchemaCodec extends JsonDecode[Schema] with JsonEncode[Schema] {
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

    def decode(x: JValue) = PSchema.matches(x) match {
      case Right(results) => Right(Schema(hashVar(results), schemaVar(results), pkVar(results)))
      case Left(ex) => Left(ex)
    }
  }
}
