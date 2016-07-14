package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JNumber}

class RowId(val underlying: Long) extends AnyVal {
  override def toString = s"RowId($underlying)"
}

object RowId {
  implicit val jCodec = new JsonDecode[RowId] with JsonEncode[RowId] {
    def encode(versionId: RowId) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case n: JNumber => Right(new RowId(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[RowId] {
    def compare(x: RowId, y: RowId): Int = Ordering.Long.compare(x.underlying, y.underlying)
  }
}
