package com.socrata.querycoordinator

import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.types.SoQLFloatingTimestamp

class TestQueryRewriterDateTruncLtGte extends TestQueryRewriterDateTruncBase {
  val rewrittenQueryRymd = "SELECT c2 as ward, sum(c3) as count WHERE c1 >= '2011-03-08' AND c1 < '2019-08-02' GROUP BY c2"
  val rewrittenQueryAnalysisRymd = analyzeRewrittenQuery("r_ymd", rewrittenQueryRymd)

  val rewrittenQueryRym = "SELECT c2 as ward, sum(c3) as count WHERE c1 >= '2011-03-01' AND c1 < '2019-08-01' GROUP BY c2"
  val rewrittenQueryAnalysisRym = analyzeRewrittenQuery("r_ymd", rewrittenQueryRym)

  val rewrittenQueryRy = "SELECT c2 as ward, sum(c3) as count WHERE c1 >= '2011-01-01' AND c1 < '2019-01-01' GROUP BY c2"
  val rewrittenQueryAnalysisRy = analyzeRewrittenQuery("r_y", rewrittenQueryRy)

  test("year") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date >= '2011-01-01' AND crime_date < '2019-01-01' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key ("r_ym")
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key ("r_y")
    rewrites.get("r_y").get should equal(rewrittenQueryAnalysisRy)

    rewrites should have size (3)
  }

  test("month") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date >= '2011-03-01' AND crime_date < '2019-08-01' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRym)
    rewrites should contain key ("r_ym")
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRym)

    rewrites should have size (2)
  }

  test("day") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date >= '2011-03-08' AND crime_date < '2019-08-02' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRymd)

    rewrites should have size (1)
  }

  // Ensure that queries explicitly filtering out dates that javascript can't handle can hit rollups.
  test("9999 filter") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date >= '2011-03-08' AND crime_date < '2019-08-02' AND crime_date < '9999-01-01' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(analyzeRewrittenQuery("r_ymd",
      "SELECT c2 as ward, sum(c3) as count WHERE c1 >= '2011-03-08' AND c1 < '2019-08-02' AND c1 < '9999-01-01' GROUP BY c2"))
    rewrites should have size (1)
  }


  // Ensure that queries explicitly filtering out nulls can hit rollups
  test("IS NOT NULL filter") {
    val q = "SELECT ward, count(*) AS count WHERE " +
      "crime_date >= '2011-03-08' AND crime_date < '2019-08-02' AND crime_date IS NOT NULL GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key ("r_ymd")
    rewrites.get("r_ymd").get should equal(analyzeRewrittenQuery("r_ymd",
      "SELECT c2 as ward, sum(c3) as count WHERE c1 >= '2011-03-08' AND c1 < '2019-08-02' AND c1 IS NOT NULL GROUP BY c2"))
    rewrites should have size (1)
  }

  test("shouldn't rewrite") {
    rewritesFor("SELECT ward WHERE crime_date < '2012-01-01T00:00:01'") should have size (0)
    rewritesFor("SELECT ward WHERE crime_date <= '2012-01-01T00:00:00'") should have size (0)
    rewritesFor("SELECT ward WHERE crime_date > '2012-01-01T00:00:00'") should have size (0)
    rewritesFor("SELECT ward WHERE crime_date > '2012-01-01T01:00:00'") should have size (0)
    rewritesFor("SELECT ward WHERE crime_date >= '2012-01-01T01:00:00' AND crime_date < '2013-01-01'") should have size (0)
  }

  test("truncatedTo") {
    def ts(s: String) = SoQLFloatingTimestamp.apply(SoQLFloatingTimestamp.StringRep.unapply(s).get)
    rewriter.truncatedTo(ts("2012-01-01")) should be (Some(FloatingTimeStampTruncY))
    rewriter.truncatedTo(ts("2012-05-01")) should be (Some(FloatingTimeStampTruncYm))
    rewriter.truncatedTo(ts("2012-05-09")) should be (Some(FloatingTimeStampTruncYmd))
    rewriter.truncatedTo(ts("2012-05-09T01:00:00")) should be (None)
    rewriter.truncatedTo(ts("2012-05-09T00:10:00")) should be (None)
    rewriter.truncatedTo(ts("2012-05-09T00:00:02")) should be (None)
    rewriter.truncatedTo(ts("2012-05-09T00:00:00.001")) should be (None)
  }

}
