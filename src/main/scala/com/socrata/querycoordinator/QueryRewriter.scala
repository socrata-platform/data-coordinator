package com.socrata.querycoordinator

import com.socrata.soql.{SoQLAnalyzer, SoQLAnalysis}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions._
import com.socrata.soql.typed._
import com.socrata.soql.types._

import scala.util.{Failure, Success, Try}

class QueryRewriter(analyzer: SoQLAnalyzer[SoQLAnalysisType]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryRewriter])
  type RollupName = String
  type ColumnId = String
  type Expr = CoreExpr[ColumnId, SoQLAnalysisType]
  type Selection = OrderedMap[ColumnName, Expr]
  type GroupBy = Option[Seq[Expr]]
  type Where = Option[Expr]
  type Anal = SoQLAnalysis[ColumnId, SoQLAnalysisType]

  def ensure(expr: Boolean, msg: String) = if(!expr) Some(msg) else None

  // TODO the secondary should probably just give us the names of the columns when we ask about the rollups
  // instead of assuming.
  private[querycoordinator] def rollupColumnId(idx: Int) = "c" + (idx+1)

  /** Maps the rollup column expression to the 0 based index in the rollup table.  If we have
    * multiple columns with the same definition, that is fine but we will only use one.
    */
  def mapSelection(q: Selection, r: Selection, rollupColIdx: Map[Expr, Int]): Option[Selection] = {
    val mapped: OrderedMap[ColumnName, Option[Expr]] = q.mapValues(e => mapExpr(e, rollupColIdx))

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
      // if the query is grouping, every grouping in the query must grouped in the rollup.
      // The analysis already validated there are no un-grouped columns in the selection
      // that aren't in the group by.
      case (Some(q), _) =>
        val grouped: Seq[Option[Expr]] = q.map { expr => mapExpr(expr, rollupColIdx) }

        if (grouped.forall(g => g.isDefined)) {
          Some(Some(grouped.flatten))
        } else {
          None
        }
      // TODO We can also rewrite if the rollup has no group bys, but need to do the right aggregate
      // manipulation.
      case _ => None
    }
  }

  /** Find the first column index that is a count(*) or count(literal). */
  def findCountStarOrLiteral(rollupColIdx: Map[Expr, Int]): Option[Int] = {
    rollupColIdx.filterKeys {
      case fc: FunctionCall[_,_]  if isCountStarOrLiteral(fc) => true
      case _ => false
    }.map(_._2).headOption
  }

  /** Is the given function call a count(*) or count(literal), excluding NULL because it is special.  */
  def isCountStarOrLiteral(fc: FunctionCall[_,_]) = {
    fc.function.function == SoQLFunctions.CountStar ||
      (fc.function.function == SoQLFunctions.Count &&
        fc.parameters.forall {
          case e : NullLiteral[_] => false
          case e : TypedLiteral[_] => true
          case _ => false
        })
  }

  /** Can this function be applied to its own output in a further aggregation */
  def isSelfAggregatableAggregate(f: Function[_]) = f match {
    case _  @ (SoQLFunctions.Max | SoQLFunctions.Min | SoQLFunctions.Sum) => true
    case _ => false
  }

  /** Recursively maps the Expr based on the rollupColIdx map, returning either
    * a mapped expression or None if the expression couldn't be mapped.
    */
  def mapExpr(e: Expr, rollupColIdx: Map[Expr, Int]): Option[Expr] = {
    log.trace("Attempting to match expr: {}", e)
    e match {
      // This is literally a literal, so so literal.
      case literal: TypedLiteral[_] => Some(literal)
      // for a column reference we just need to map the column id
      case cr: ColumnRef[ColumnId, SoQLAnalysisType] =>
        for (idx <- rollupColIdx.get(cr)) yield cr.copy(column = rollupColumnId(idx))(cr.position)
      // A count(*) or count(non-null-literal) on q gets mapped to a sum on any such column in rollup
      case fc: FunctionCall[_, _] if isCountStarOrLiteral(fc) =>
        val mf = MonomorphicFunction(SoQLFunctions.Sum, Map("a" -> SoQLNumber))
        for {
          idx <- findCountStarOrLiteral(rollupColIdx)  // find count(*) column in rollup
          newSumCol <- Some(ColumnRef[ColumnId, SoQLAnalysisType](rollupColumnId(idx), SoQLNumber)(fc.position))
          newFc <- Some(FunctionCall[ColumnId, SoQLAnalysisType](mf, Seq(newSumCol))(fc.position, fc.position))
        } yield newFc
      // Other non-aggregation functions can be recursively mapped
      case fc: FunctionCall[ColumnId, SoQLAnalysisType] if
          fc.function.isAggregate == false =>
        val mapped = fc.parameters.map(fe => mapExpr(fe, rollupColIdx))
        log.trace("mapped expr params {} {} -> {}", "", fc.parameters, mapped)
        if (mapped.forall(fe => fe.isDefined)) {
          log.trace("expr params all defined")
          Some(fc.copy(parameters = mapped.flatten)(fc.position, fc.position))
        } else {
          None
        }
      // If the function is "self aggregatable" we can apply it on top of an already aggregated rollup
      // column, eg. select foo, bar, max(x) max_x group by foo, bar --> select foo, max(max_x) group by foo
      // If we have a matching column, we just need to update its argument to reference the rollup column.
      case fc: FunctionCall[ColumnId, SoQLAnalysisType] if  isSelfAggregatableAggregate(fc.function.function) =>
        for {
          idx <- rollupColIdx.get(fc)
        } yield fc.copy(parameters =
            Seq(ColumnRef[ColumnId, SoQLAnalysisType](rollupColumnId(idx), fc.typ)(fc.position)))(fc.position, fc.position)

      case _ => None
    }
  }

  def mapWhere(qeOpt: Option[Expr], r: Anal, rollupColIdx: Map[Expr, Int]): Option[Where] = {
    log.debug(s"Attempting to map query where expression '${qeOpt}' to rollup ${r}")

    (qeOpt, r.where) match {
      // don't support rollups with where clauses yet.  To do so, we need to validate that r.where
      // is also contained in q.where.
      case (_, Some(re)) => None
      // no where on query or rollup, so good!  No work to do.
      case (None, None) => Some(None)
      // have a where on query so try to map recursively
      case (Some(qe), None) => mapExpr(qe, rollupColIdx).map(Some(_))
    }
  }


  def mapOrderBy(obsOpt: Option[Seq[OrderBy[ColumnId, SoQLAnalysisType]]], rollupColIdx: Map[Expr, Int]):
      Option[Option[Seq[OrderBy[ColumnId, SoQLAnalysisType]]]] = {
    log.debug(s"Attempting to map order by expression '${obsOpt}'")

    // it is silly if the rollup has an order by, but we really don't care.
    obsOpt match {
      case Some(obs) =>
        val mapped = obs.map { ob =>
          mapExpr(ob.expression, rollupColIdx) match {
            case Some(e) => Some(ob.copy(expression = e))
            case None => None
          }
        }
        if (mapped.forall { ob => ob.isDefined }) {
          Some(Some(mapped.flatten))
        } else {
          None
        }
      case None => Some(None)
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
      val where = mapWhere(q.where, r, rollupColIdx)
      val orderBy = mapOrderBy(q.orderBy, rollupColIdx)

      val mismatch =
          ensure(selection.isDefined, "mismatch on select") orElse
          ensure(where.isDefined, "mismatch on where") orElse
          ensure(groupBy.isDefined, "mismatch on groupBy") orElse
          ensure(q.having == r.having, "mismatch on having") orElse
          ensure(orderBy.isDefined, "mismatch on orderBy") orElse
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
            orderBy = orderBy.get,
            where = where.get,
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

    analysisMap.foreach {
      case (k, Failure(e)) => log.warn(s"Couldn't parse rollup '${rollupMap.get(k).get}'", e)
      case _ =>
    }

    analysisMap collect {
      case (k, Success(a)) => k -> a
    }
  }
}
