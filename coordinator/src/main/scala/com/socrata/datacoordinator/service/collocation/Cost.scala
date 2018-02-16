package com.socrata.datacoordinator.service.collocation

import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, JsonKeyStrategy, Strategy}

@JsonKeyStrategy(Strategy.Underscore)
case class Cost(moves: Int, totalSizeBytes: Long, moveSizeMaxBytes: Option[Long] = None) {
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

  val Zero: Cost = Cost(moves = 0, totalSizeBytes = 0L, moveSizeMaxBytes = Some(0L))

  // since we have not yet backfilled the secondary_metrics table and we don't want to
  // outright fail, return an obviously not real value for the total size in bytes.
  val Unknown: Cost = Cost(moves = 1, totalSizeBytes = -1L)
}

// Parameters must all be non-negative and
// for Cost.Zero < Cost.Unknown we must have:
//
// movesWeight > totalSizeBytesWeight + moveSizeMaxBytesWeight
// and movesWeight > 0.0
case class WeightedCostOrdering(movesWeight: Double,
                                totalSizeBytesWeight: Double,
                                moveSizeMaxBytesWeight: Double) extends Ordering[Cost] {

  if (movesWeight <= 0.0) throw new IllegalArgumentException("movesWeight must be positive")
  if (totalSizeBytesWeight < 0.0) throw new IllegalArgumentException("totalSizeBytesWeight must be non-negative")
  if (moveSizeMaxBytesWeight < 0.0) throw new IllegalArgumentException("moveSizeMaxBytesWeight must be non-negative")

  if (movesWeight <= totalSizeBytesWeight + moveSizeMaxBytesWeight)
    throw new IllegalArgumentException("movesWeight > totalSizeBytesWeight + moveSizeMaxBytesWeight, " +
      s"but $movesWeight <= $totalSizeBytesWeight + $moveSizeMaxBytesWeight")

  private def weightedCost(cost: Cost): Double = {
    cost.moves * movesWeight +
      cost.totalSizeBytes * totalSizeBytesWeight +
      cost.getMoveSizeMaxBytes * moveSizeMaxBytesWeight
  }

  override def compare(x: Cost, y: Cost): Int = {
    weightedCost(x) compare weightedCost(y)
  }
}
