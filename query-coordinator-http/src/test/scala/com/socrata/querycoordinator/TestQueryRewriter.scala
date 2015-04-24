package com.socrata.querycoordinator

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType

class TestQueryRewriter extends TestQueryRewriterBase {
  /** Each rollup here is defined by:
    * - a name
    * - a soql statement.  Note this must be the mapped statement, ie. non-system columns prefixed by an _, and backtick escaped
    * - a Seq of the soql types for each column in the rollup selection
    */
  val rollups = Seq(
    ("r1", "SELECT `_dxyz-num1`, count(`_dxyz-num1`) GROUP BY `_dxyz-num1`"),
    ("r2", "SELECT count(`:wido-ward`), `:wido_ward` GROUP BY `:wido-ward`"),
    ("r3", "SELECT `:wido-ward`, count(*) GROUP BY `:wido-ward`"),
    ("r4", "SELECT `:wido-ward`, `_crim-typ3`, count(*), `_dxyz-num1`, `_crim-date` GROUP BY `:wido-ward`, `_crim-typ3`, `_dxyz-num1`, `_crim-date`"),
    ("r5", "SELECT `_crim-typ3`, count(1) group by `_crim-typ3`"),
    ("r6", "SELECT `:wido-ward`, `_crim-typ3`"),
    ("r7", "SELECT `:wido-ward`, min(`_dxyz-num1`), max(`_dxyz-num1`), sum(`_dxyz-num1`), count(*) GROUP BY `:wido-ward`"),
    ("r8", "SELECT date_trunc_ym(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_ym(`_crim-date`), `:wido-ward`")
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

  test("map query ward, count(*)") {
    val q = "SELECT ward, count(*) AS ward_count GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c1 AS ward, sum(c3) AS ward_count GROUP by c1"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r4", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysis)

    rewrites should contain key("r3")
    rewrites should contain key("r7")
    rewrites should contain key("r8")
    rewrites should have size(4)
  }

  test("map query crime_type, ward, count(*)") {
    val q = "SELECT crime_type, ward, count(*) AS ward_count GROUP BY crime_type, ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c2 AS crime_type, c1 as ward, sum(c3) AS ward_count GROUP by c2, c1"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r4", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysis)

    rewrites should have size(1)
  }

  // count(null) is very different than count(*)!
  test("don't map count(null) - query crime_type, ward, count(null)") {
    val q = "SELECT crime_type, ward, count(null) AS ward_count GROUP BY crime_type, ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should have size(0)
  }


  test("shouldn't rewrite column not in rollup") {
    val q = "SELECT ward, dont_create_rollups, count(*) AS ward_count GROUP BY ward, dont_create_rollups"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should be(empty)
  }


  test("hidden column aliasing") {
    val q = "SELECT crime_type as crimey, ward as crime_type"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c1 as crimey, c1 as crime_type"
    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r6", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis )

    rewrites should contain key("r6")
    rewrites.get("r6").get should equal(rewrittenQueryAnalysis)
  }


  test("map query crime_type, ward, 1, count(*) with LIMIT / OFFSET") {
    val q = "SELECT crime_type, ward, 1, count(*) AS ward_count GROUP BY crime_type, ward LIMIT 100 OFFSET 200"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c2 AS crime_type, c1 as ward, 1, sum(c3) AS ward_count GROUP by c2, c1 LIMIT 100 OFFSET 200"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r4", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysis)

    rewrites should have size(1)
  }


  test("count on literal - map query crime_type, count(0), count('potato')") {
    val q = "SELECT crime_type, count(0) as crimes, count('potato') as crimes_potato GROUP BY crime_type"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    val rewrittenQueryR4 = "SELECT c2 AS crime_type, sum(c3) as crimes, sum(c3) as crimes_potato GROUP by c2"
    val rewrittenQueryAnalysisR4 = analyzeRewrittenQuery("r4", rewrittenQueryR4)
    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysisR4)

    val rewrittenQueryR5 = "SELECT c1 AS crime_type, sum(c2) as crimes, sum(c2) as crimes_potato GROUP by c1"
    val rewrittenQueryAnalysisR5 = analyzeRewrittenQuery("r5", rewrittenQueryR5)
    rewrites should contain key("r5")
    rewrites.get("r5").get  should equal(rewrittenQueryAnalysisR5)

    // TODO should be 3 eventually... should also rewrite from table w/o group by
    rewrites should have size(2)
  }

  test("map query ward, count(*) where") {
    val q = "SELECT ward, count(*) AS ward_count WHERE crime_type = 'Clownicide' AND number1 > 5 GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c1 AS ward, sum(c3) AS ward_count WHERE c2 = 'Clownicide' AND c4 > 5 GROUP by c1"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r4", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysis)

    rewrites should have size(1)
  }


  test("order by - query crime_type, ward, count(*)") {
    val q = "SELECT crime_type, ward, count(*) AS ward_count GROUP BY crime_type, ward ORDER BY count(*) desc, crime_type"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c2 AS crime_type, c1 as ward, sum(c3) AS ward_count GROUP by c2, c1 ORDER BY sum(c3) desc, c2"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r4", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysis)

    rewrites should have size(1)
  }


  test("map query ward, date_trunc_ym(crime_date), count(*)") {
    val q = "SELECT ward, date_trunc_ym(crime_date) as d, count(*) AS ward_count GROUP BY ward, date_trunc_ym(crime_date)"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQueryR4 = "SELECT c1 AS ward, date_trunc_ym(c5) as d, sum(c3) AS ward_count GROUP by c1, date_trunc_ym(c5)"

    val rewrittenQueryAnalysisR4 = analyzeRewrittenQuery("r4", rewrittenQueryR4)

    // in this case, we map the function call directly to the column ref
    val rewrittenQueryR8 = "SELECT c2 as ward, c1 as d, sum(c3) as ward_count GROUP BY c2, c1"
    val rewrittenQueryAnalysisR8= analyzeRewrittenQuery("r8", rewrittenQueryR8)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysisR4)

    rewrites should contain key("r8")
    rewrites.get("r8").get  should equal(rewrittenQueryAnalysisR8)

    rewrites should have size(2)
  }

  test("map query ward, max(n), min(n), count(*)") {
    val q = "SELECT ward, max(number1) as max_num, min(number1) as min_num, count(*) AS ward_count GROUP BY ward"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c1 AS ward, max(c3) as max_num, min(c2) as min_num, sum(c5) AS ward_count GROUP by c1"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r7", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r7")
    rewrites.get("r7").get  should equal(rewrittenQueryAnalysis)

    // TODO should be 2 eventually... should also rewrite from table w/o group by
//    rewrites should contain key("r4")

    rewrites should have size(1)
  }

  test("Query count(0) without group by") {
    val q = "SELECT count(0) as countess WHERE crime_type = 'NARCOTICS'"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    val rewrittenQueryR4 = "SELECT sum(c3) as countess WHERE c2 = 'NARCOTICS'"
    val rewrittenQueryAnalysisR4 = analyzeRewrittenQuery("r4", rewrittenQueryR4)
    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysisR4)

    val rewrittenQueryR5 = "SELECT sum(c2) as countess WHERE c1 = 'NARCOTICS'"
    val rewrittenQueryAnalysisR5 = analyzeRewrittenQuery("r5", rewrittenQueryR5)
    rewrites should contain key("r5")
    rewrites.get("r5").get  should equal(rewrittenQueryAnalysisR5)

    rewrites should have size(2)
  }

  test("Query min/max without group by") {
    val q = "SELECT min(number1) as minn, max(number1) as maxn WHERE ward = 7"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    val rewrittenQuery = "SELECT min(c2) as minn, max(c3) as maxn WHERE c1 = 7"
    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r7", rewrittenQuery)
    rewrites should contain key("r7")
    rewrites.get("r7").get  should equal(rewrittenQueryAnalysis)

    rewrites should have size(1)
  }
}
