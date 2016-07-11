package com.socrata.datacoordinator.id

import com.rojoma.json.v3.util.WrapperJsonCodec

case class StrategyType(underlying: String) extends AnyVal

object StrategyType extends (String => StrategyType) {
  implicit val jCodec = WrapperJsonCodec[StrategyType](StrategyType, _.underlying)

  implicit val ordering = new Ordering[StrategyType] {
    def compare(x: StrategyType, y: StrategyType): Int = Ordering.String.compare(x.underlying, y.underlying)
  }
}
