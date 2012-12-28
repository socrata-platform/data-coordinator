package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNumber}

class ColumnId(val underlying: Long) extends AnyVal
object ColumnId {
  implicit val jCodec = new JsonCodec[ColumnId] {
    def encode(versionId: ColumnId) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case JNumber(n) => Some(new ColumnId(n.toLong))
      case _ => None
    }
  }

  implicit val ordering = new Ordering[ColumnId] {
    def compare(x: ColumnId, y: ColumnId): Int = Ordering.Long.compare(x.underlying, y.underlying)
  }
}
