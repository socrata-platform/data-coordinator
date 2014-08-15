package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString}

class RollupName(val underlying: String) extends AnyVal {
  override def toString = s"RollupName($underlying)"
}

object RollupName {
  implicit val jCodec = new JsonCodec[RollupName] {
    def encode(name: RollupName) = JString(name.underlying)
    def decode(v: JValue) = v match {
      case JString(s) => Some(new RollupName(s))
      case _ => None
    }
  }

  implicit val ordering = new Ordering[RollupName] {
    def compare(x: RollupName, y: RollupName): Int = x.underlying.compareTo(y.underlying)
  }
}
