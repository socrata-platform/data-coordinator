package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JNumber}

class RowVersion(val underlying: Long) extends AnyVal {
  override def toString = s"RowVersion($underlying)"
}

object RowVersion {
  implicit val jCodec = new JsonDecode[RowVersion] with JsonEncode[RowVersion] {
    def encode(versionId: RowVersion) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case n: JNumber => Right(new RowVersion(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[RowVersion] {
    def compare(x: RowVersion, y: RowVersion): Int = Ordering.Long.compare(x.underlying, y.underlying)
  }
}
