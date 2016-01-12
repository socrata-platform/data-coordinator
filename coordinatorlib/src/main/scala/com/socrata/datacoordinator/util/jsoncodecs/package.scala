package com.socrata.datacoordinator.util

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.socrata.soql.environment.ColumnName

package object jsoncodecs {
  implicit object ColumnNameCodec extends JsonEncode[ColumnName] with JsonDecode[ColumnName] {
    override def encode(x: ColumnName): JValue = JString(x.name)

    override def decode(x: JValue): DecodeResult[ColumnName] = x match {
      case JString(s) => Right(ColumnName(s))
      case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
    }
  }
}
