package com.socrata.datacoordinator.service.collocation

import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, JsonKeyStrategy, Strategy}

@JsonKeyStrategy(Strategy.Underscore)
case class Cost(moves: Int, totalSizeBytes: Long, moveSizeMaxBytes: Option[Long] = None) extends Ordered[Cost] {
  override def compare(that: Cost): Int = {
    this.moves - that.moves
  }

  def getMoveSizeMaxBytes: Long = this.moveSizeMaxBytes.getOrElse(this.totalSizeBytes)

  def +(that: Cost): Cost = {
    Cost(
      moves = this.moves + that.moves,
      totalSizeBytes = this.totalSizeBytes + that.totalSizeBytes,
      moveSizeMaxBytes = Some(Math.max(this.getMoveSizeMaxBytes, that.getMoveSizeMaxBytes))
    )
  }
}

object Cost {
  implicit val encode = AutomaticJsonEncodeBuilder[Cost]

  def max(x: Cost, y: Cost): Cost = Ordering[Cost].max(x, y)

  val Zero: Cost = Cost(moves = 0, totalSizeBytes = 0L, moveSizeMaxBytes = Some(0L))

  val Unknown: Cost = Cost(moves = 1, totalSizeBytes = Long.MinValue)
}
