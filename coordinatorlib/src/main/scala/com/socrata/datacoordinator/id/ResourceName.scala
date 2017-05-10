package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JString}

class ResourceName(val underlying: String) {
  override def toString = s"ResourceName($underlying)"
}

object ResourceName {
  implicit val jCodec = new JsonDecode[ResourceName] with JsonEncode[ResourceName] {
    def encode(name: ResourceName) = JString(name.underlying)
    def decode(v: JValue) = v match {
      case JString(s) => Right(new ResourceName(s))
      case other      => Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[ResourceName] {
    def compare(x: ResourceName, y: ResourceName): Int = x.underlying.compareTo(y.underlying)
  }
}
