package com.socrata.querycoordinator.util

import com.rojoma.json.ast.{JValue, JString}
import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.SoQLType

object SoQLTypeCodec extends JsonCodec[SoQLType] {
  def encode(t: SoQLType) = JString(t.name.name)
  def decode(v: JValue) = v match {
    case JString(s) => SoQLType.typesByName.get(TypeName(s))
    case _ => None
  }
}