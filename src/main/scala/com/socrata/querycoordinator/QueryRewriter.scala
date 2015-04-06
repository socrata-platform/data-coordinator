package com.socrata.querycoordinator

import org.joda.time.{DateTimeConstants, LocalDateTime}

import scala.util.{Failure, Success, Try}

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions._
import com.socrata.soql.typed
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}

class QueryRewriter(analyzer: SoQLAnalyzer[SoQLAnalysisType]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryRewriter])
  type RollupName = String
  type ColumnId = String
  type Expr = typed.CoreExpr[ColumnId, SoQLAnalysisType]
  type Selection = OrderedMap[ColumnName, Expr]
  type GroupBy = Option[Seq[Expr]]
  type OrderBy = typed.OrderBy[ColumnId, SoQLAnalysisType]
  type Where = Option[Expr]
  type Anal = SoQLAnalysis[ColumnId, SoQLAnalysisType]
  type FunctionCall = typed.FunctionCall[ColumnId, SoQLAnalysisType]
  type ColumnRef = typed.ColumnRef[ColumnId, SoQLAnalysisType]

  def ensure(expr: Boolean, msg: String) = if(!expr) Some(msg) else None

  // TODO the secondary should probably just give us the names of the columns when we ask about the rollups
  // instead of assuming.
  private[querycoordinator] def rollupColumnId(idx: Int) = "c" + (idx+1)

  /** Maps the rollup column expression to the 0 based index in the rollup table.  If we have
    * multiple columns with the same definition, that is fine but we will only use one.
    */
  def rewriteSelection(q: Selection, r: Selection, rollupColIdx: Map[Expr, Int]): Option[Selection] = {
    val mapped: OrderedMap[ColumnName, Option[Expr]] = q.mapValues(e => rewriteExpr(e, rollupColIdx))

    if (mapped.values.forall(c => c.isDefined)) {
      Some(mapped.mapValues { v => v.get })
    } else {
      None
    }
  }

  def rewriteGroupBy(qOpt: GroupBy, rOpt: GroupBy, rollupColIdx: Map[Expr, Int]): Option[GroupBy] = {
    (qOpt, rOpt) match {
      // If the query has no group by, then either the rollup has no group by so they match, or
      // the rollup does have one, in which case the analysis will ensure that if there are
      // any aggregate functions in the selection, then all of the other columns are compatible
      // (ie. no columnrefs without aggregate functions).
      case (None, _) => Some(None)
      // if the query is grouping, every grouping in the query must grouped in the rollup.
      // The analysis already validated there are no un-grouped columns in the selection
      // that aren't in the group by.
      case (Some(q), _) =>
        val grouped: Seq[Option[Expr]] = q.map { expr => rewriteExpr(expr, rollupColIdx) }

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
    rollupColIdx.find {
      case (fc: FunctionCall, _) => isCountStarOrLiteral(fc)
      case _ => false
    }.map(_._2)
  }

  /** Is the given function call a count(*) or count(literal), excluding NULL because it is special.  */
  def isCountStarOrLiteral(fc: FunctionCall) = {
    fc.function.function == CountStar ||
      (fc.function.function == Count &&
        fc.parameters.forall {
          case e : NullLiteral[_] => false
          case e : TypedLiteral[_] => true
          case _ => false
        })
  }

  /** Can this function be applied to its own output in a further aggregation */
  def isSelfAggregatableAggregate(f: Function[_]) = f match {
    case Max | Min | Sum => true
    case _ => false
  }

  /**
   * Looks at the rollup columns supplied and tries to find one of the supplied functions whose first parameter operates
   * on the given ColumnRef.  If there are multiple matches, returns the first matching function.  Every function supplied
   * must take at least one parameter.
   */
  private def findFunctionOnColumn(rollupColIdx: Map[Expr, Int], functions: Seq[Function[_]], colRef: ColumnRef): Option[Int] = {
    val possibleColIdxs = functions.map { function =>
      rollupColIdx.find {
        case (fc: FunctionCall, _) if fc.function.function == function && fc.parameters.head == colRef => true
        case _ => false
      }.map(_._2)
    }
    possibleColIdxs.flatten.headOption
  }

  /** An in order hierarchy of floating timestamp date truncation functions, from least granular to most granular.  */
  private val dateTruncHierarchy = Seq(FloatingTimeStampTruncY,
                                       FloatingTimeStampTruncYm,
                                       FloatingTimeStampTruncYmd)

  /**
   * This tries to rewrite a between expression on floating timestamps to use an aggregated rollup column.
   * These functions have a hierarchy, so a query for a given level of truncation can be served by any other
   * truncation that is at least as long, eg. ymd can answer ym queries.
   *
   * This is slightly different than the more general expression rewrites because BETWEEN returns a boolean, so
   * there is no need to apply the date aggregation function on the ColumnRef.  In fact, we do not want to
   * apply the aggregation function on the ColumnRef because that will end up being bad for query execution
   * performance in most cases.
   *
   * @param fc A NOT BETWEEN or BETWEEN function call.
   */
  private def rewriteDateTruncBetweenExpr(rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    assert(fc.function.function == Between || fc.function.function == NotBetween)
    val maybeColRef +: lower +: upper +: _ = fc.parameters

    /** The common date truncation function shared between the lower and upper bounds of the between */
    val commonTruncFunction = (lower, upper) match {
      case (lowerFc: FunctionCall, upperFc: FunctionCall) =>
        (lowerFc.function.function, upperFc.function.function) match {
          case (l, u) if l == u && dateTruncHierarchy.contains(l) => Some(l)
          case _ => None
        }
      case _ => None
    }

    /** The column index in the rollup that matches the common truncation function, either exactly or at a more granular level */
    val colIdx = maybeColRef match {
      case colRef: ColumnRef if colRef.typ == SoQLFloatingTimestamp =>
        for {
          // we can rewrite to any date_trunc_xx that is the same or after the desired one in the hierarchy
          possibleTruncFunctions <- commonTruncFunction.map { tf => dateTruncHierarchy.dropWhile { f => f != tf } }
          idx <- findFunctionOnColumn(rollupColIdx, possibleTruncFunctions, colRef)
        } yield idx
      case _ => None
    }

    /** Now rewrite the BETWEEN to use the rollup, if possible. */
    colIdx match {
      case Some(idx: Int) =>
        // All we have to do is replace the first argument with the rollup column reference since it is just being used
        // as a comparison that result in a boolean, then rewrite the b and c expressions.
        // ie. 'foo_date BETWEEN date_trunc_y("2014/01/01") AND date_trunc_y("2019/05/05")' just has to replace foo_date with
        // rollup column "c<n>"
        for {
          lowerRewrite <- rewriteExpr(lower, rollupColIdx)
          upperRewrite <- rewriteExpr(upper, rollupColIdx)
          newParams <- Some(Seq(typed.ColumnRef(rollupColumnId(idx), SoQLFloatingTimestamp)(fc.position), lowerRewrite, upperRewrite))
        } yield fc.copy(parameters = newParams)(fc.position, fc.position)
      case _ => None
    }
  }

  /**
   * Returns the least granular date truncation function that can be applied to the timestamp
   * without changing its value.
   */
  private[querycoordinator] def truncatedTo(soqlTs: SoQLFloatingTimestamp): Option[Function[SoQLFloatingTimestamp.type]] = {
    val ts: LocalDateTime = soqlTs.value
    if (ts.getMillisOfDay != 0) {
      None
    } else if (ts.getDayOfMonth != 1) {
      Some(FloatingTimeStampTruncYmd)
    } else if (ts.getMonthOfYear != DateTimeConstants.JANUARY) {
      Some(FloatingTimeStampTruncYm)
    } else {
      Some(FloatingTimeStampTruncY)
    }
  }

  /**
   * Rewrite "less than" and "greater to or equal" to use rollup columns.  Note that date_trunc_xxx functions
   * are a form of a floor function.  This means that date_trunc_xxx(column) >= value will always be
   * the same as column >= value iff value could have been output by date_trunc_xxx.  Similar holds
   * for Lt, only it has to be strictly less since floor rounds down.
   *
   * For example, column >= '2014-03-01' AND column < '2015-05-01' can be rewritten as
   * date_trunc_ym(column) >= '2014-03-01' AND date_trunc_ym(column) < '2015-05-01' without changing
   * the results.
   *
   * Note that while we wouldn't need any of the logic here if queries explicitly came in as filtering
   * on date_trunc_xxx(column), we do not want to encourage that form of querying since is typically
   * much more expensive when you can't hit a rollup table.
   */
  private def rewriteDateTruncGteLt(rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    fc.function.function match {
      case Lt | Gte =>
        val left +: right +: _ = fc.parameters
        (left, right) match {
          // The left hand side should be a floating timestamp, and the right hand side will be a string being cast
          // to a floating timestamp.  eg. my_floating_timestamp < '2010-01-01'::floating_timestamp
          // While it is eminently reasonable to also accept them in flipped order, that is being left for later.
          case (colRef@ColumnRef(_, SoQLFloatingTimestamp),
                cast@FunctionCall(MonomorphicFunction(TextToFloatingTimestamp, _), Seq(StringLiteral(ts, _)))) =>
            for {
              parsedTs <- SoQLFloatingTimestamp.StringRep.unapply(ts)
              truncatedTo <- truncatedTo(SoQLFloatingTimestamp(parsedTs))
              // we can rewrite to any date_trunc_xx that is the same or after the desired one in the hierarchy
              possibleTruncFunctions <- Some(dateTruncHierarchy.dropWhile { f => f != truncatedTo })
              rollupColIdx <- findFunctionOnColumn(rollupColIdx, possibleTruncFunctions, colRef)
              newParams <- Some(Seq(typed.ColumnRef(rollupColumnId(rollupColIdx), SoQLFloatingTimestamp)(fc.position), right))
            } yield fc.copy(parameters = newParams)(fc.position, fc.position)
          case _ => None
        }
      case _ => None
    }
  }

  /** Recursively maps the Expr based on the rollupColIdx map, returning either
    * a mapped expression or None if the expression couldn't be mapped.
    *
    * Note that every case here needs to ensure to map every expression recursively
    * to ensure it is either a literal or mapped to the rollup.
    */
  def rewriteExpr(e: Expr, rollupColIdx: Map[Expr, Int]): Option[Expr] = {
    log.trace("Attempting to match expr: {}", e)
    e match {
      // This is literally a literal, so so literal.
      case literal: TypedLiteral[_] => Some(literal)

      // for a column reference we just need to map the column id
      case cr: ColumnRef =>
        for (idx <- rollupColIdx.get(cr)) yield cr.copy(column = rollupColumnId(idx))(cr.position)

      // A count(*) or count(non-null-literal) on q gets mapped to a sum on any such column in rollup
      case fc: FunctionCall if isCountStarOrLiteral(fc) =>
        val mf = MonomorphicFunction(Sum, Map("a" -> SoQLNumber))
        for {
          idx <- findCountStarOrLiteral(rollupColIdx)  // find count(*) column in rollup
          newSumCol <- Some(typed.ColumnRef(rollupColumnId(idx), SoQLNumber)(fc.position))
          newFc <- Some(typed.FunctionCall(mf, Seq(newSumCol))(fc.position, fc.position))
        } yield newFc

      // If this is a between function operating on floating timestamps, and arguments b and c are both date aggregates,
      // then try to rewrite argument a to use a rollup.
      case fc: FunctionCall
          if (fc.function.function == Between || fc.function.function == NotBetween) &&
             fc.function.bindings.values.forall(_ == SoQLFloatingTimestamp) &&
             fc.function.bindings.values.tail.forall(dateTruncHierarchy contains _) =>
        rewriteDateTruncBetweenExpr(rollupColIdx, fc)

      // If it is a >= or < with floating timestamp arguments, see if we can rewrite to date_trunc_xxx
      case fc@FunctionCall(MonomorphicFunction(fnType, bindings), _)
            if (fnType == Gte || fnType == Lt) &&
                bindings.values.forall(_ == SoQLFloatingTimestamp) =>
        rewriteDateTruncGteLt(rollupColIdx, fc)

      // Not null on a column can be translated to not null on a date_trunc_xxx(column)
      // There is actually a much more general case on this where non-aggregate functions can
      // be applied on top of other non-aggregate functions in many cases that we are not currently
      // implementing.
      case fc@FunctionCall(MonomorphicFunction(IsNotNull, _), Seq(colRef@ColumnRef(_, _)))
            if findFunctionOnColumn(rollupColIdx, dateTruncHierarchy, colRef).isDefined =>
        for {
          colIdx <- findFunctionOnColumn(rollupColIdx, dateTruncHierarchy, colRef)
        } yield fc.copy(parameters =  Seq(ColumnRef(rollupColumnId(colIdx), colRef.typ)(fc.position)))(fc.position, fc.functionNamePosition)

      // remaining non-aggregate functions
      case fc: FunctionCall if fc.function.isAggregate == false =>
        // if we have the exact same function in rollup and query, just turn it into a column ref in the rollup
        val functionMatch = for {
          idx <- rollupColIdx.get(fc)
        } yield typed.ColumnRef(rollupColumnId(idx), fc.typ)(fc.position)

        // otherwise, see if we can recursively rewrite
        functionMatch.orElse {
          val mapped = fc.parameters.map(fe => rewriteExpr(fe, rollupColIdx))
          log.trace("mapped expr params {} {} -> {}", "", fc.parameters, mapped)
          if (mapped.forall(fe => fe.isDefined)) {
            log.trace("expr params all defined")
            Some(fc.copy(parameters = mapped.flatten)(fc.position, fc.position))
          } else {
            None
          }
        }

      // If the function is "self aggregatable" we can apply it on top of an already aggregated rollup
      // column, eg. select foo, bar, max(x) max_x group by foo, bar --> select foo, max(max_x) group by foo
      // If we have a matching column, we just need to update its argument to reference the rollup column.
      case fc: FunctionCall if  isSelfAggregatableAggregate(fc.function.function) =>
        for {
          idx <- rollupColIdx.get(fc)
        } yield fc.copy(parameters =
            Seq(typed.ColumnRef(rollupColumnId(idx), fc.typ)(fc.position)))(fc.position, fc.position)

      case _ => None
    }
  }

  def rewriteWhere(qeOpt: Option[Expr], r: Anal, rollupColIdx: Map[Expr, Int]): Option[Where] = {
    log.debug(s"Attempting to map query where expression '${qeOpt}' to rollup ${r}")

    (qeOpt, r.where) match {
      // don't support rollups with where clauses yet.  To do so, we need to validate that r.where
      // is also contained in q.where.
      case (_, Some(re)) => None
      // no where on query or rollup, so good!  No work to do.
      case (None, None) => Some(None)
      // have a where on query so try to map recursively
      case (Some(qe), None) => rewriteExpr(qe, rollupColIdx).map(Some(_))
    }
  }


  def rewriteOrderBy(obsOpt: Option[Seq[OrderBy]], rollupColIdx: Map[Expr, Int]):
      Option[Option[Seq[OrderBy]]] = {
    log.debug(s"Attempting to map order by expression '${obsOpt}'")

    // it is silly if the rollup has an order by, but we really don't care.
    obsOpt match {
      case Some(obs) =>
        val mapped = obs.map { ob =>
          rewriteExpr(ob.expression, rollupColIdx) match {
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

      val selection = rewriteSelection(q.selection, r.selection, rollupColIdx)
      val groupBy = rewriteGroupBy(q.groupBy, r.groupBy, rollupColIdx)
      val where = rewriteWhere(q.where, r, rollupColIdx)
      val orderBy = rewriteOrderBy(q.orderBy, rollupColIdx)

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
    QueryParser.dsContext(columnIdMap , schema.schema)
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
