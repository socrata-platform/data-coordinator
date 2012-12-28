package com.socrata.datacoordinator.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNumber}

class DatasetId(val underlying: Long) extends AnyVal
object DatasetId {
  implicit val jCodec = new JsonCodec[DatasetId] {
    def encode(datasetId: DatasetId) = JNumber(datasetId.underlying)
    def decode(v: JValue) = v match {
      case JNumber(n) => Some(new DatasetId(n.toLong))
      case _ => None
    }
  }
}
