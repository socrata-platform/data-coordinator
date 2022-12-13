package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.ast.{JNumber, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}

case class RollupId(val underlying: Long) {
  override def toString = s"RollupId($underlying)"
}

object RollupId {
  implicit val jCodec = new JsonDecode[RollupId] with JsonEncode[RollupId] {
    def encode(rollupId: RollupId) = JNumber(rollupId.underlying)

    def decode(v: JValue): Either[DecodeError, RollupId] = v match {
      case n: JNumber => Right(new RollupId(n.toLong))
      case other => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }
}
