package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNumber}

class CopyId(val underlying: Long) extends AnyVal {
  override def toString = s"CopyId($underlying)"
}

object CopyId {
  implicit val jCodec = new JsonCodec[CopyId] {
    def encode(versionId: CopyId) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case JNumber(n) => Some(new CopyId(n.toLong))
      case _ => None
    }
  }
}
