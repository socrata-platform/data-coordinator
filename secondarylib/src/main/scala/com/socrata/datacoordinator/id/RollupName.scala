package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JString}

class RollupName(val underlying: String) extends AnyVal {
  override def toString = s"RollupName($underlying)"
}

object RollupName {
  implicit val jCodec = new JsonDecode[RollupName] with JsonEncode[RollupName] {
    def encode(name: RollupName) = JString(name.underlying)
    def decode(v: JValue) = v match {
      case JString(s) => Right(new RollupName(s))
      case other      => Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[RollupName] {
    def compare(x: RollupName, y: RollupName): Int = x.underlying.compareTo(y.underlying)
  }
}
