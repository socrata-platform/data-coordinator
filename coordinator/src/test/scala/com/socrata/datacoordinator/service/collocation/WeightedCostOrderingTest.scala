package com.socrata.datacoordinator.service.collocation

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.{FunSuite, Matchers}

class WeightedCostOrderingTest extends FunSuite with Matchers {

  test("A WeightedCostOrdering must have all non-negative parameters and movesWeight must be positive") {
    intercept[IllegalArgumentException] {
      WeightedCostOrdering(0.0, 0.25, 0.5)
    }.getMessage should be ("movesWeight must be positive")

    intercept[IllegalArgumentException] {
      WeightedCostOrdering(1.0, -0.25, 0.5)
    }.getMessage should be ("totalSizeBytesWeight must be non-negative")

    intercept[IllegalArgumentException] {
      WeightedCostOrdering(1.0, 0.25, -0.5)
    }.getMessage should be ("moveSizeMaxBytesWeight must be non-negative")
  }

  test("A WeightedCostOrdering must have movesWeight > totalSizeBytesWeight + moveSizeMaxBytesWeight") {
    intercept[IllegalArgumentException] {
      WeightedCostOrdering(1.0, 1.0, 1.0)
    }.getMessage should be ("movesWeight > totalSizeBytesWeight + moveSizeMaxBytesWeight, but 1.0 <= 1.0 + 1.0")
  }

  // movesWeight > totalSizeBytesWeight + moveSizeMaxBytesWeight
  def genOrdering: Gen[Ordering[Cost]] = for {
    movesWeight <- Gen.posNum[Double]
    totalSizeBytesWeight <- Gen.posNum[Double].suchThat(_ < movesWeight)
    moveSizeMaxBytesWeight <- Gen.posNum[Double].suchThat(_ < movesWeight - totalSizeBytesWeight)
  } yield {
    WeightedCostOrdering(movesWeight, totalSizeBytesWeight, moveSizeMaxBytesWeight)
  }

  def genKnownCost: Gen[Cost] = for {
    moves <- Gen.posNum[Int]
    totalSizeBytes <- Gen.posNum[Long]
    moveSizeMaxBytes <- Gen.posNum[Long].suchThat(_ <= totalSizeBytes)
  } yield {
    Cost(moves, totalSizeBytes, Some(moveSizeMaxBytes))
  }

  test("Cost.Zero should always be less than Cost.Unknown") {
    forAll(genOrdering) { ordering =>
      ordering.compare(Cost.Zero, Cost.Unknown) < 0
    }
  }

  test("Cost.Zero should always be less than known costs") {
    forAll(genOrdering) { ordering =>
      forAll(genKnownCost) { knownCost =>
        ordering.compare(Cost.Zero, knownCost) < 0
      }
    }
  }

  test("Cost.Unknown should always be less than known costs") {
    forAll(genOrdering) { ordering =>
      forAll(genKnownCost) { knownCost =>
        ordering.compare(Cost.Unknown, knownCost) < 0
      }
    }
  }

  test("The same known costs should be equal") {
    forAll(genOrdering) { ordering =>
      forAll(genKnownCost) { knownCost =>
        ordering.compare(knownCost, knownCost) == 0
      }
    }
  }

  test("For known costs only differing in the number of moves, " +
    "the one with less moves should be less than the other") {
    forAll(genOrdering) { ordering =>
      val genCosts = for {
        movesX <- Gen.posNum[Int]
        movesY <- Gen.posNum[Int].suchThat(_ != movesX)
        totalSizeBytes <- Gen.posNum[Long]
        moveSizeMaxBytes <- Gen.posNum[Long].suchThat(_ <= totalSizeBytes)
      } yield {
        (Cost(movesX, totalSizeBytes, Some(moveSizeMaxBytes)), Cost(movesY, totalSizeBytes, Some(moveSizeMaxBytes)))
      }

      forAll(genCosts) { case (costX, costY) =>
        (ordering.compare(costX, costY) < 0) == (costX.moves < costY.moves)
      }
    }
  }

  test("For known costs only differing in the total size, " +
    "the one with the smaller total size should be less than the other") {
    forAll(genOrdering) { ordering =>
      val genCosts = for {
        moves <- Gen.posNum[Int]
        totalSizeBytesX <- Gen.posNum[Long]
        totalSizeBytesY <- Gen.posNum[Long].suchThat(_ != totalSizeBytesX)
        moveSizeMaxBytes <- Gen.posNum[Long].suchThat(_ <= Math.min(totalSizeBytesX, totalSizeBytesY))
      } yield {
        (Cost(moves, totalSizeBytesX, Some(moveSizeMaxBytes)), Cost(moves, totalSizeBytesY, Some(moveSizeMaxBytes)))
      }

      forAll(genCosts) { case (costX, costY) =>
        (ordering.compare(costX, costY) < 0) == (costX.totalSizeBytes < costY.totalSizeBytes)
      }
    }
  }

  test("For known costs only differing in the max move size, " +
    "the one with the smaller max move size should be less than the other") {
    forAll(genOrdering) { ordering =>
      val genCosts = for {
        moves <- Gen.posNum[Int]
        totalSizeBytes <- Gen.posNum[Long]
        moveSizeMaxBytesX <- Gen.posNum[Long].suchThat(_ <= totalSizeBytes)
        moveSizeMaxBytesY <- Gen.posNum[Long].suchThat(_ <= totalSizeBytes).suchThat(_ != moveSizeMaxBytesX)
      } yield {
        (Cost(moves, totalSizeBytes, Some(moveSizeMaxBytesX)), Cost(moves, totalSizeBytes, Some(moveSizeMaxBytesY)))
      }

      forAll(genCosts) { case (costX, costY) =>
        (ordering.compare(costX, costY) < 0) == (costX.getMoveSizeMaxBytes < costY.getMoveSizeMaxBytes)
      }
    }
  }
}
