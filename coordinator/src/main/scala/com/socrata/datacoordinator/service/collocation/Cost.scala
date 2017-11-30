package com.socrata.datacoordinator.service.collocation

import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, JsonKeyStrategy, Strategy}

@JsonKeyStrategy(Strategy.Underscore)
case class Cost(moves: Int) extends Ordered[Cost] {
  override def compare(that: Cost): Int = {
    this.moves - that.moves
  }

  def +(that: Cost): Cost = {
    Cost(moves = this.moves + that.moves)
  }
}

object Cost {
  implicit val encode = AutomaticJsonEncodeBuilder[Cost]

  val Zero: Cost = Cost(moves = 0)
}
