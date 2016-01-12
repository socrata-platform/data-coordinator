package com.socrata.datacoordinator.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonEncode, JsonDecode}

class StrategyType(val underlying: String) extends AnyVal {
  override def toString = s"StrategyType($underlying)"
}

object StrategyType {
  implicit val jCodec = new JsonDecode[StrategyType] with JsonEncode[StrategyType] {
    def encode(strategyType: StrategyType) = JString(strategyType.underlying)
    def decode(v: JValue): Either[DecodeError, StrategyType] = v match {
      case n: JString => Right(new StrategyType(n.string))
      case other      => Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }

  implicit val ordering = new Ordering[StrategyType] {
    def compare(x: StrategyType, y: StrategyType): Int = Ordering.String.compare(x.underlying, y.underlying)
  }
}
