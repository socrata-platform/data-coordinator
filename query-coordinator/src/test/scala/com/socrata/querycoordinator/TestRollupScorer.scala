package com.socrata.querycoordinator

class TestRollupScorer extends TestQueryRewriterBase {

  test("rollup scoring") {
    /** Each rollup here is defined by:
      * - a name
      * - a soql statement.  Note this must be the mapped statement,
      * i.e. non-system columns prefixed by an _, and backtick escaped
      * - a Seq of the soql types for each column in the rollup selection
      *
      * They need to be ORDERED in "best" to worst order and all have distinct scores to ensure there are
      * no ties.
      */
    val rollups = Seq(
      ("r1", "SELECT sum(`_dxyz-num1`)"),
      ("r2", "SELECT `_dxyz-num1`, count(`_dxyz-num1`) WHERE `_dxyz-num1` = 1 AND `_crim-date` IS NOT NULL AND `:wido-ward` = 8 GROUP BY `_dxyz-num1` "),
      ("r3", "SELECT `_dxyz-num1`, count(`_dxyz-num1`) WHERE `_dxyz-num1` = 1 AND `_crim-date` IS NOT NULL GROUP BY `_dxyz-num1`"),
      ("r4", "SELECT `_dxyz-num1`, count(`_dxyz-num1`) WHERE `_crim-date` IS NOT NULL GROUP BY `_dxyz-num1`"),
      ("r5", "SELECT `_dxyz-num1`, count(`_dxyz-num1`) GROUP BY `_dxyz-num1`"),
      ("r6", "SELECT `:wido-ward`, min(`_dxyz-num1`), max(`_dxyz-num1`), sum(`_dxyz-num1`), count(*) GROUP BY `:wido-ward`"),
      ("r7", "SELECT date_trunc_ym(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_ym(`_crim-date`), `:wido-ward`"),
      ("r8", "SELECT `:wido-ward`, `_crim-typ3`, count(*), `_dxyz-num1`, `_crim-date` GROUP BY `:wido-ward`, `_crim-typ3`, `_dxyz-num1`, `_crim-date`"),
      ("r9", "SELECT `:wido-ward`"),
      ("ra", "SELECT `:wido-ward`, `_crim-typ3`")
    )

    val rollupInfos = rollups.map { x => new RollupInfo(x._1, x._2) }

    /** Pull in the rollupAnalysis for easier debugging */
    val rollupAnalysis = rewriter.analyzeRollups(schema, rollupInfos)

    // validate we don't have any ties, which could confuse things due to having no clear ordering.
    rollupAnalysis.values.map(RollupScorer.scoreRollup(_)).toSet should have size rollups.size

    val sorted = RollupScorer.sortByScore(rollupAnalysis.toSeq)

    sorted.map(_._1) should equal (rollups.map(_._1))

    RollupScorer.bestRollup(rollupAnalysis.toSeq).map(_._1) should equal (Some("r1"))
  }
}
