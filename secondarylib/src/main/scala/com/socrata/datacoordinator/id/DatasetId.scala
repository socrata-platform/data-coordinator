package com.socrata.datacoordinator.id

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JValue, JNumber}

class DatasetId(val underlying: Long) extends AnyVal {
  override def toString = s"DatasetId($underlying)"
}

object DatasetId {
  implicit val jCodec = new JsonDecode[DatasetId] with JsonEncode[DatasetId] {
    def encode(datasetId: DatasetId) = JNumber(datasetId.underlying)
    def decode(v: JValue) = v match {
      case n: JNumber => Right(new DatasetId(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }

  val Invalid = new DatasetId(-1)
}
