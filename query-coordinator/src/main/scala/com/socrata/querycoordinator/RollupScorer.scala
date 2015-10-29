package com.socrata.querycoordinator

import com.socrata.soql.functions.SoQLFunctions
import com.typesafe.scalalogging.slf4j.Logging

/**
 * A very trivial rule based optimizer that assigns scores to rollups to try to determine which is the best one to
 * query given multiple options.  In a world of great complexity this approach is more or less doomed to failure
 * due to not having enough information, however it is simple to implement the simple cases and can go a long way and
 * we don't need to be at the level of sophisitication of an advanced cost based optimizer.  If we ever do we are
 * probably barking up the wrong tree.
 *
 * All decisions are made off the structure of the rollup with no insight into the actual size of the rollup
 * or the actual query execution plan.
 *
 * A score is assigned by adding up the following laughingly simplistic rules:
 *
 * - Number of GROUP BY clauses
 *  - 0
 *   - If all functions are aggregates, 0 since this should be a one line rollup!
 *   - Else 100000 * the number of columns in the selection to greatly penalize this non-rollup rollup
 *  - >0 then 100 * the number of GROUP BYs, with the assumption each one increases the size of the rollup
 * - For each "top level" AND clause in the WHERE clause, reduce the score by 10 points with the assumption each one
 *   makes the rollup smaller.
 * - For each value in the selection, give a minor penalty of 1
 *
 */
import QueryRewriter._

private object RollupScorer extends Logging {
  private val SELECTION_SCORE_PENALTY = -10L

  def scoreRollup(r: Anal): Long = {
      val whereScore = r.where match {
        case Some(w) => SELECTION_SCORE_PENALTY /* for a single filter */ +
          scoreExpr(w) /* recursively look for ANDs */
        case _ => 0
    }
    val score = scoreGroup(r) +
      1 * r.selection.size /* slight penalty for more columns in the selection */ +
      whereScore

    logger.trace(s"Scored rollup $r as $score (whereScore=$whereScore)}")
    score
  }

  def scoreGroup(r: Anal): Long = {
    r.groupBy match {
      case None =>
        r.selection.forall {
          case (_, fc: FunctionCall) if fc.function.isAggregate => true
          case _ => false
        } match {
          // everything is an aggregate so this should be one row, nice!
          case true => 0
          // no aggregation, stiff penalty so we don't use this unless we have to
          case false => 100000 * r.selection.size
        }
      // assume each group by increases the size.  This is assuming a linear increase even
      // though it typically isn't that simple.
      case Some(g) => 100 * g.length
    }
  }

  def scoreExpr(e: Expr): Long = {
    val score = e match {
      case fc: FunctionCall if fc.function.function == SoQLFunctions.And =>
        fc.parameters.foldLeft(SELECTION_SCORE_PENALTY)(_ + scoreExpr(_))
      case _ => 0
    }
    logger.trace(s"Scored value of $score for expr $e")
    score
  }

  /** Returns a sorted Seq of rollups, from best to worst. */
  def sortByScore(rollups: Seq[(RollupName, Anal)]): Seq[(RollupName, Anal)] = {
    rollups.sortBy(r => RollupScorer.scoreRollup(r._2))
  }

  def bestRollup(rollups: Seq[(RollupName, Anal)]): Option[(RollupName, Anal)] = {
    sortByScore(rollups).headOption
  }
}
