package com.socrata.querycoordinator.util

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.DecodeError.{InvalidType, InvalidValue}
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.SoQLType

object SoQLTypeCodec extends JsonDecode[SoQLType] with JsonEncode[SoQLType] {
  def encode(t: SoQLType) = JString(t.name.name)

  def decode(v: JValue) = v match {
    case JString(s) => SoQLType.typesByName.get(TypeName(s)).map(Right(_)).getOrElse(Left(InvalidValue(v)))
    case _ => Left(InvalidType(JString, v.jsonType))
  }
}