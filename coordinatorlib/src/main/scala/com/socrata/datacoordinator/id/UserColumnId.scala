package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.util.WrapperFieldCodec

class UserColumnId(val underlying: String) extends AnyVal {
  override def toString = s"UserColumnId($underlying)"
}

object UserColumnId {
  implicit val jCodec = new JsonDecode[UserColumnId] with JsonEncode[UserColumnId] {
    def encode(cid: UserColumnId) = JString(cid.underlying)
    def decode(v: JValue) = v match {
      case JString(s) => Right(new UserColumnId(s))
      case other      => Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }

  implicit val fieldCodec = WrapperFieldCodec[UserColumnId](new UserColumnId(_), _.underlying)

  implicit val ordering = new Ordering[UserColumnId] {
    def compare(x: UserColumnId, y: UserColumnId): Int = x.underlying.compareTo(y.underlying)
  }
}
