package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNumber}

class RowVersion(val underlying: Long) extends AnyVal {
  override def toString = s"RowVersion($underlying)"
}

object RowVersion {
  implicit val jCodec = new JsonCodec[RowVersion] {
    def encode(versionId: RowVersion) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case JNumber(n) => Some(new RowVersion(n.toLong))
      case _ => None
    }
  }

  implicit val ordering = new Ordering[RowVersion] {
    def compare(x: RowVersion, y: RowVersion): Int = Ordering.Long.compare(x.underlying, y.underlying)
  }
}
