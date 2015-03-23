package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JNumber}

class ColumnId(val underlying: Long) extends AnyVal {
  override def toString = s"ColumnId($underlying)"
}

object ColumnId {
  implicit val jCodec = new JsonDecode[ColumnId] with JsonEncode[ColumnId] {
    def encode(versionId: ColumnId) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case n: JNumber => Right(new ColumnId(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[ColumnId] {
    def compare(x: ColumnId, y: ColumnId): Int = Ordering.Long.compare(x.underlying, y.underlying)
  }
}
