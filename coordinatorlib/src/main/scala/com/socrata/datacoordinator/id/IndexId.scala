package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JNumber}

class IndexId(val underlying: Long) extends AnyVal {
  override def toString = s"IndexId($underlying)"
}

object IndexId {
  implicit val jCodec = new JsonDecode[IndexId] with JsonEncode[IndexId] {
    def encode(versionId: IndexId) = JNumber(versionId.underlying)
    def decode(v: JValue): Either[DecodeError, IndexId] = v match {
      case n: JNumber => Right(new IndexId(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }

  val Invalid = new IndexId(-1)
}
