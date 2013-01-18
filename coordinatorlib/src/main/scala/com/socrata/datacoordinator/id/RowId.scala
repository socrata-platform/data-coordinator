package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNumber}

class RowId(val underlying: Long) extends AnyVal {
  override def toString = s"RowId($underlying)"
}

object RowId {
  implicit val jCodec = new JsonCodec[RowId] {
    def encode(versionId: RowId) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case JNumber(n) => Some(new RowId(n.toLong))
      case _ => None
    }
  }

  implicit val ordering = new Ordering[RowId] {
    def compare(x: RowId, y: RowId): Int = Ordering.Long.compare(x.underlying, y.underlying)
  }

  val initial = new RowId(2)
}
