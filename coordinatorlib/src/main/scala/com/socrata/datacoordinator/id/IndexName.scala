package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JString}

class IndexName(val underlying: String) extends AnyVal {
  override def toString = s"IndexName($underlying)"
}

object IndexName {
  implicit val jCodec = new JsonDecode[IndexName] with JsonEncode[IndexName] {
    def encode(name: IndexName) = JString(name.underlying)
    def decode(v: JValue) = v match {
      case JString(s) => Right(new IndexName(s))
      case other      => Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[IndexName] {
    def compare(x: IndexName, y: IndexName): Int = x.underlying.compareTo(y.underlying)
  }
}
