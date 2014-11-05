package com.socrata.querycoordinator

import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.SoQLType

class TestQueryRewriterDateTrunc extends TestQueryRewriterBase {
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

  val rewrittenQueryRymd = "SELECT c2 as ward, sum(c3) as count WHERE c1 BETWEEN date_trunc_ymd('2011-02-01') AND date_trunc_ymd('2012-05-02') GROUP BY c2"
  val rewrittenQueryAnalysisRymd = analyzeRewrittenQuery("r_ymd", rewrittenQueryRymd)

  val rewrittenQueryRym = "SELECT c2 as ward, sum(c3) as count WHERE c1 BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02') GROUP BY c2"
  val rewrittenQueryAnalysisRym = analyzeRewrittenQuery("r_ymd", rewrittenQueryRym)

  val rewrittenQueryNotBetweenRym = "SELECT c2 as ward, sum(c3) as count WHERE c1 NOT BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02') GROUP BY c2"
  val rewrittenQueryAnalysisNotBetweenRym = analyzeRewrittenQuery("r_ymd", rewrittenQueryNotBetweenRym)

  val rewrittenQueryRy = "SELECT c2 as ward, sum(c3) as count WHERE c1 BETWEEN date_trunc_y('2011-02-01') AND date_trunc_y('2012-05-02') GROUP BY c2"
  val rewrittenQueryAnalysisRy = analyzeRewrittenQuery("r_y", rewrittenQueryRy)

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

  test("Shouldn't rewrite date_trunc on literals") {
    val q = "SELECT ward, count(*) as count WHERE crime_date BETWEEN '2004-01-01' AND '2014-01-01' GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size (0)
  }

  test("Shouldn't rewrite date_trunc on differing aggregations") {
    val q = "SELECT ward, count(*) as count WHERE crime_date BETWEEN date_trunc_y('2004-01-01') AND date_trunc_ym('2014-01-01') GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size (0)
  }

  test("Shouldn't rewrite date_trunc on expression that can't be mapped") {
    val q = "SELECT ward, count(*) as count WHERE crime_date BETWEEN date_trunc_y(some_date) AND date_trunc_y('2014-01-01') GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size (0)
  }

  test("BETWEEN date_trunc_ymd") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date BETWEEN date_trunc_ymd('2011-02-01') AND date_trunc_ymd('2012-05-02') GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRymd)

    rewrites should have size (1)
  }


  test("BETWEEN date_trunc_ym") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02') GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRym)
    rewrites should contain key ("r_ym")
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRym)

    rewrites should have size (2)
  }


  test("BETWEEN date_trunc_y") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date BETWEEN date_trunc_y('2011-02-01') AND date_trunc_y('2012-05-02') GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key ("r_ym")
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key ("r_y")
    rewrites.get("r_y").get should equal(rewrittenQueryAnalysisRy)

    rewrites should have size (3)
  }


  test("NOT BETWEEN date_trunc_ym") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date NOT BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02') GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisNotBetweenRym)
    rewrites should contain key ("r_ym")
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisNotBetweenRym)

    rewrites should have size (2)
  }
}
