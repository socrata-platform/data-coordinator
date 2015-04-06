package com.socrata.querycoordinator

import com.socrata.soql.environment.ColumnName

class TestQueryRewriterDateTruncBase extends TestQueryRewriterBase {
  /** Each rollup here is defined by:
    * - a name
    * - a soql statement.  Note this must be the mapped statement, ie. non-system columns prefixed by an _, and backtick escaped
    * - a Seq of the soql types for each column in the rollup selection
    */
  val rollups = Seq(
    ("r_ymd", "SELECT date_trunc_ymd(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_ymd(`_crim-date`), `:wido-ward`"),
    ("r_ym", "SELECT date_trunc_ym(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_ym(`_crim-date`), `:wido-ward`"),
    ("r_y", "SELECT date_trunc_y(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_y(`_crim-date`), `:wido-ward`")
  )

  val rollupInfos = rollups.map { x => new RollupInfo(x._1, x._2)}

  /** Pull in the rollupAnalysis for easier debugging */
  val rollupAnalysis = rewriter.analyzeRollups(schema, rollupInfos)

  val rollupRawSchemas = rollupAnalysis.mapValues { case analysis =>
    analysis.selection.values.toSeq.zipWithIndex.map { case (expr, idx) =>
      rewriter.rollupColumnId(idx) -> expr.typ.canonical
    }.toMap
  }

  /** Analyze a "fake" query that has the rollup table column names in, so we
    * can use it to compare  with the rewritten one in assertions.
    */
  def analyzeRewrittenQuery(rollupName: String, q: String) = {
    val rewrittenRawSchema = rollupRawSchemas(rollupName)

    val rollupNoopColumnNameMap = rewrittenRawSchema.map { case (k, v) => ColumnName(k) -> k}

    val rollupDsContext = QueryParser.dsContext(rollupNoopColumnNameMap, rewrittenRawSchema)

    val rewrittenQueryAnalysis = analyzer.analyzeFullQuery(q)(rollupDsContext).mapColumnIds(rollupNoopColumnNameMap)
    rewrittenQueryAnalysis
  }

  def rewritesFor(q: String) = rewriter.possibleRewrites(analyzeQuery(q), rollupAnalysis)
}
