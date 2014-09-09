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
  type RollupName = String
  type ColumnId = String
  type Expr = CoreExpr[ColumnId, SoQLAnalysisType]
  type Selection = OrderedMap[ColumnName, Expr]
  type GroupBy = Option[Seq[Expr]]
  type Anal = SoQLAnalysis[ColumnId, SoQLAnalysisType]

  def ensure(expr: Boolean, msg: String) = if(!expr) Some(msg) else None

  // TODO the secondary should probably just give us the names of the columns when we ask about the rollups
  // instead of assuming.
  private[querycoordinator] def rollupColumnId(idx: Int) = "c" + (idx+1)

  /** Maps the rollup column expression to the 0 based index in the rollup table.  If we have
    * multiple columns with the same definition, that is fine but we will only use one.
    */
  def mapSelection(q: Selection, r: Selection, rollupColIdx: Map[Expr, Int]): Option[Selection] = {
    def mapExpr(expr: Expr): Option[Expr] = {
      expr match {
        // literal is as literal will.
        case literal: TypedLiteral[_] => Some(literal)
        // if the column in the query is in the rollup, map directly
        case cr: ColumnRef[ColumnId, SoQLAnalysisType] =>
          for (idx <- rollupColIdx.get(cr)) yield cr.copy(column = rollupColumnId(idx))(cr.position)
        // A count(*) in both q and r gets mapped to a sum(c<x>) on rollup table
        case fc: FunctionCall[_, _] if fc.function.function == SoQLFunctions.CountStar =>
          val mf = MonomorphicFunction(SoQLFunctions.Sum, Map("a" -> SoQLNumber))
          for {
            idx <- rollupColIdx.get(fc) // find count(*) column in rollup
            newSumCol <- Some(ColumnRef[ColumnId, SoQLAnalysisType](rollupColumnId(idx), SoQLNumber)(fc.position))
            newFc <- Some(FunctionCall[ColumnId, SoQLAnalysisType](mf, Seq(newSumCol))(fc.position, fc.position))
          } yield newFc
        case _ => None
      }
    }

    val mapped: OrderedMap[ColumnName, Option[Expr]] = q.mapValues(mapExpr)

    if (mapped.values.forall(c => c.isDefined)) {
      Some(mapped.mapValues { v => v.get })
    } else {
      None
    }
  }

  def mapGroupBy(qOpt: GroupBy, rOpt: GroupBy, rollupColIdx: Map[Expr, Int]): Option[GroupBy] = {
    (qOpt, rOpt) match {
      // neither have group by, so we match on no group by
      case (None, None) => Some(None)
      case (Some(q), Some(r)) =>
        val grouped: Seq[Option[Expr]] = q.map { expr =>
          expr match {
            // if the grouping column in the query is in the rollup, map directly
            case cr: ColumnRef[ColumnId, SoQLAnalysisType] =>
              for (idx <- rollupColIdx.get(cr)) yield cr.copy(column = rollupColumnId(idx))(cr.position)
            case _ => None
          }
        }
        if (grouped.forall(g => g.isDefined)) {
          Some(Some(grouped.flatten))
        } else {
          None
        }
      case _ => None
    }
  }


  def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal]): Map[RollupName, Anal] = {
    log.debug("looking for candidates to rewrite for query: {}", q)
    val candidates = rollups.mapValues { r =>
      log.debug("checking for compat with: {}", r)

      // this lets us lookup the column and get the 0 based index in the select list
      val rollupColIdx = r.selection.values.zipWithIndex.toMap

      val selection = mapSelection(q.selection, r.selection, rollupColIdx)
      val groupBy = mapGroupBy(q.groupBy, r.groupBy, rollupColIdx)

      val mismatch =
          ensure(selection.isDefined, "mismatch on select") orElse
          ensure(q.where == r.where, "mismatch on where") orElse
          ensure(groupBy.isDefined, "mismatch on groupBy") orElse
          ensure(q.having == r.having, "mismatch on having") orElse
          ensure(q.orderBy == r.orderBy, "mismatch on orderBy") orElse
          // For limit and offset, we can always apply them from the query  as long as the rollup
          // doesn't have any.  For certain cases it would be possible to rewrite even if the rollup
          // has a limit or offset, but we currently don't.
          ensure(None == r.limit, "mismatch on limit") orElse
          ensure(None == r.offset, "mismatch on offset") orElse
          ensure(q.search == None, "mismatch on search")

      mismatch match {
        case None =>
          Some(r.copy(
            selection = selection.get,
            groupBy = groupBy.get,
            limit = q.limit,
            offset = q.offset
          ))
        case Some(s) =>
          log.debug("Not compatible: {}", s)
          None
      }
    }

    log.debug("Final candidates: {}", candidates)
    candidates.collect { case (k,Some(v)) => k -> v }
  }

  def bestRewrite(q: Anal, rollups: Map[RollupName,Anal]): Option[(RollupName, Anal)] = {
    possibleRewrites(q, rollups).headOption
  }

  def bestRewrite(schema: Schema, q: Anal, rollups: Seq[RollupInfo]): Option[(RollupName, Anal)] = {
    possibleRewrites(q, analyzeRollups(schema, rollups)).headOption
  }

  /**
   * For analyzing the rollup query, we need to map the dataset schema column ids to the "_" prefixed
   * version of the name that we get, designed to ensure the column name is valid soql
   * and doesn't start with a number.
   */
  private def addRollupPrefix(name: ColumnId): ColumnName = {
    new ColumnName(if (name(0) == ':') name else "_" + name)
  }


  /**
   * Once we have the analyzed rollup query with column names, we need to remove the leading "_" on non-system
   * columns to make the names match up with the underlying dataset schema.
   */
  private def removeRollupPrefix(cn: ColumnName): ColumnId = {
    cn.name(0) match {
      case ':' => cn.name
      case _ => cn.name.drop(1)
    }
  }

  // maps prefixed column name to type
  private def prefixedDsContext(schema: Schema) = {
    val columnIdMap = schema.schema.map { case (k,v) => addRollupPrefix(k) -> k }
    QueryParser.dsContext(columnIdMap , schema.schema) match {
      case Right(ctx) => ctx
      case Left(unknownColumnIds) =>
        // Ok, we are cheating here to avoid properly exposing this error .  Thing is we shouldn't be able to
        // get here without the query already being parsed, and the query can't be parsed if there are
        // unknown column ids.
        throw new RuntimeException("Found ColumnIds in the name mapping that aren't in the schema, but the QueryParser should have already caught this!")
    }
  }

  def analyzeRollups(schema: Schema, rollups: Seq[RollupInfo]): Map[RollupName,Anal] = {
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
