package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNumber}

class VersionId(val underlying: Long) extends AnyVal
object VersionId {
  implicit val jCodec = new JsonCodec[VersionId] {
    def encode(versionId: VersionId) = JNumber(versionId.underlying)
    def decode(v: JValue) = v match {
      case JNumber(n) => Some(new VersionId(n.toLong))
      case _ => None
    }
  }
}
