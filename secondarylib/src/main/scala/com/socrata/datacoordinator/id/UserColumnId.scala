package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString}

class UserColumnId(val underlying: String) extends AnyVal {
  override def toString = s"UserColumnId($underlying)"
}

object UserColumnId {
  implicit val jCodec = new JsonCodec[UserColumnId] {
    def encode(cid: UserColumnId) = JString(cid.underlying)
    def decode(v: JValue) = v match {
      case JString(s) => Some(new UserColumnId(s))
      case _ => None
    }
  }

  implicit val ordering = new Ordering[UserColumnId] {
    def compare(x: UserColumnId, y: UserColumnId): Int = x.underlying.compareTo(y.underlying)
  }
}
