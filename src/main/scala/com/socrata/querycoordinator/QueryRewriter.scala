package com.socrata.querycoordinator

import com.socrata.soql.{SoQLAnalyzer, SoQLAnalysis}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.functions._
import com.socrata.soql.typed.{TypedLiteral, FunctionCall, ColumnRef, CoreExpr}
import com.socrata.soql.types._

import scala.util.{Success, Try}

class QueryRewriter(analyzer: SoQLAnalyzer[SoQLAnalysisType]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryRewriter])
  type Selection = OrderedMap[ColumnName, CoreExpr[String, SoQLAnalysisType]]
  type Expr = CoreExpr[String, SoQLAnalysisType]

  def ensure(expr: Boolean, msg: String) = if(!expr) Some(msg) else None

  // TODOMS the secondary should probably just give us the names of the columns when we ask about the rollups
  // instead of assuming
  private def rollupColumnId(idx: Int) = "c" + (idx+1)

  def mapSelection(q: Selection, r: Selection, rollupColIdx: Map[Expr, Int]): Option[Selection] = {
    /** Maps the rollup column expression to the 0 based index in the rollup table.  If we have
      * multiple columns with the same definition, that is fine but we will only use one.
      */

    val mapped: OrderedMap[ColumnName, Option[Expr]] = q.mapValues { expr =>
      expr match {
        // literal is as literal will.
        case literal: TypedLiteral[_] => Some(literal)
        // if the column in the query is in the rollup, map directly
        case cr: ColumnRef[String, SoQLAnalysisType] =>
          for (idx <- rollupColIdx.get(cr)) yield cr.copy(column = rollupColumnId(idx))(cr.position)
        // A count(*) in both q and r gets mapped to a sum(c<x>) on rollup table
        case fc: FunctionCall[_, _] if fc.function.function == SoQLFunctions.CountStar =>
          // TODOMS better way of building these
          val mf = MonomorphicFunction(SoQLFunctions.Sum, Map("a" -> SoQLNumber))
          val x = for {
            idx <- rollupColIdx.get(fc)
            newSumCol <- Some(ColumnRef[String, SoQLAnalysisType](rollupColumnId(idx), SoQLNumber)(fc.position))
            newFc <- Some(FunctionCall[String, SoQLAnalysisType](mf, Seq(newSumCol))(fc.position, fc.position))
          } yield newFc
          x
        case _ => None
      }
    }

    if (mapped.values.forall(c => c.isDefined)) {
      Some(mapped.mapValues { v => v.get })
    } else {
      None
    }
  }

  type GroupBy = Option[Seq[Expr]]
  def mapGroupBy(qOpt: GroupBy, rOpt: GroupBy, rollupColIdx: Map[Expr, Int]): Option[GroupBy] = {
    (qOpt, rOpt) match {
      // neither have group by, so we match on no group by
      case (None, None) => Some(None)
      case (Some(q), Some(r)) =>
        val grouped: Seq[Option[Expr]] = q.map { expr =>
          expr match {
            // if the grouping column in the query is in the rollup, map directly
            case cr: ColumnRef[String, SoQLAnalysisType] =>
              for (idx <- rollupColIdx.get(cr)) yield cr.copy(column = rollupColumnId(idx))(cr.position)
            case _ => None
          }
        }
        if (grouped.forall(g => g.isDefined)) {
          // TODOMS clean up some some maybe
          Some(Some(grouped.map { g => g.get }))
        } else {
          None
        }
      // TODOMS match on Some(q) / None(r) and test
      // group by on one, not the other... no match.
      case _ => None
    }
  }


  // TODOMS use type here?
  type Anal = SoQLAnalysis[String, SoQLAnalysisType]
  def possibleRewrites(q: Anal, rollups: Map[String,Anal]): Map[String,Anal] = {
    log.debug("looking for candidates to rewrite for query: {}", q)
    val candidates = rollups.mapValues { r =>
      log.debug("checking for compat with: {}", r)

      // this lets us lookup the column and get the 0 based index in the select list
      val rollupColIdx = r.selection.values.zipWithIndex.toMap

      val selection = mapSelection(q.selection, r.selection, rollupColIdx)
      val groupBy = mapGroupBy(q.groupBy, r.groupBy, rollupColIdx)

      val mismatch =
        ensure(q.isGrouped == r.isGrouped, "mismatch is grouped (unnecessary?") orElse
          // TODOMS we can't check this since it obviously won't match, but double check
          // there are no other issues ignoring it
          //          ensure(q.groupBy == r.groupBy, "mismatch groupby") orElse
          ensure(selection.isDefined, "mismatch select") orElse
          ensure(q.where == r.where, "mismatch where") orElse
          ensure(groupBy.isDefined, "mismatch groupBy") orElse
          ensure(q.having == r.having, "mismatch having") orElse
          ensure(q.orderBy == r.orderBy, "mismatch orderby") orElse
          ensure(q.limit == r.limit, "mismatch limit") orElse
          ensure(q.offset == r.offset, "mismatch offset") orElse
          ensure(q.search == None, "mismatch search")

      mismatch match {
        case None =>
          Some(r.copy(
            selection = selection.get,
            groupBy = groupBy.get
          ))
        case Some(s) =>
          log.debug("Not compatible: {}", s)
          None
      }
    }

    log.debug("Final candidates: {}", candidates)
    candidates.collect { case (k,Some(v)) => k -> v }
  }

  def bestRewrite(q: Anal, rollups: Map[String,Anal]): Option[(String, Anal)] = {
    possibleRewrites(q, rollups).headOption
  }

  def bestRewrite(schema: Schema, q: Anal, rollups: Seq[RollupInfo]): Option[(String, Anal)] = {
    possibleRewrites(q, analyzeRollups(schema, rollups)).headOption
  }

  /**
   * For analyzing the rollup query, we need to map the dataset schema column ids to the "_" prefixed
   * version of the name that we get, designed to ensure the column name is valid soql
   * and doesn't start with a number.
   */
  private def addRollupPrefix(name: String): ColumnName = {
    new ColumnName(if (name(0) == ':') name else "_" + name)
  }


  /**
   * Once we have the analyzed rollup query with column names, we need to remove the leading "_" on non-system
   * columns to make the names match up with the underlying dataset schema.
   */
  private def removeRollupPrefix(cn: ColumnName): String = {
    cn.name(0) match {
      case ':' => cn.name
      case _ => cn.name.drop(1)
    }
  }

  // maps prefixed column name to type
  private def prefixedDsContext(schema: Schema): DatasetContext[SoQLAnalysisType] = {
    val columnIdMap = schema.schema.map { case (k,v) => addRollupPrefix(k) -> k }
    QueryParser.dsContext(columnIdMap , schema.schema) match {
      case Right(ctx) => ctx
      case Left(missingRows) => throw new RuntimeException("MISSING: " + missingRows)
    }
  }

  def analyzeRollups(schema: Schema, rollups: Seq[RollupInfo]): Map[String,Anal] = {
    val dsContext = prefixedDsContext(schema)
    val rollupMap = rollups.map { r => (r.name, r.soql) }.toMap
    val analysisMap = rollupMap.mapValues { soql =>
      Try(analyzer.analyzeFullQuery(soql)(dsContext).mapColumnIds(removeRollupPrefix))
    }

    analysisMap collect { case (k, Success(a)) =>
      k -> a
    }
  }
}
