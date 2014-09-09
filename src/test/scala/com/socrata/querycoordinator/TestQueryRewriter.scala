package com.socrata.querycoordinator

import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.SoQLType

class TestQueryRewriter extends TestBase {
  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter = new QueryRewriter(analyzer)

  /** The raw of the table that we get as part of the secondary /schema call */
  val rawSchema = Map[String, SoQLType](
    "dxyz-num1" -> SoQLType.typesByName(TypeName("number")),
    "wido-ward" -> SoQLType.typesByName(TypeName("number")),
    "crim-typ3" -> SoQLType.typesByName(TypeName("text")),
    "dont-roll" -> SoQLType.typesByName(TypeName("text"))
  )

  /** QueryRewriter wants a Schema object to have a little stronger typing, so make one */
  val schema = Schema("NOHASH", rawSchema, "NOPK")

  /** Mapping from column name to column id, that we get from soda fountain with the query.  */
  private val columnIdMapping = Map[ColumnName, rewriter.ColumnId](
    ColumnName("number1") -> "dxyz-num1",
    ColumnName("ward") -> "wido-ward",
    ColumnName("crime_type") -> "crim-typ3",
    ColumnName("dont_create_rollups") -> "dont-roll"
  )

  /** The dataset context, used for parsing the query */
  private val dsContext = QueryParser.dsContext(columnIdMapping, rawSchema) match {
    case Right(ctx) => ctx
    case Left(unknownColumnIds) => throw new RuntimeException("Unknown column ids:" + unknownColumnIds)
  }

  /** Each rollup here is defined by:
    * - a name
    * - a soql statement.  Note this must be the mapped statement, ie. non-system columns prefixed by an _, and backtick escaped
    * - a Seq of the soql types for each column in the rollup selection
    */
  val rollups = Seq(
    ("r1", "SELECT `_dxyz-num1`, count(`_dxyz-num1`) GROUP BY `_dxyz-num1`"),
    ("r2", "SELECT count(`_wido-ward`), `_wido_ward` GROUP BY `_wido-ward`"),
    ("r3", "SELECT `_wido-ward`, count(*) GROUP BY `_wido-ward`"),
    ("r4", "SELECT `_wido-ward`, `_crim-typ3`, count(*) GROUP BY `_wido-ward`, `_crim-typ3`"),
    ("r5", "SELECT `_crim-typ3`, count(*) group by `_crim-typ3`"),
    ("r6", "SELECT `_wido-ward`, `_crim-typ3`")
  )

  val rollupInfos = rollups.map { x => new RollupInfo(x._1, x._2)}

  /** Pull in the rollupAnalysis for easier debugging */
  val rollupAnalysis = rewriter.analyzeRollups(schema, rollupInfos)

  val rollupRawSchemas = rollupAnalysis.mapValues { case analysis =>
    analysis.selection.values.toSeq.zipWithIndex.map { case (expr, idx) =>
      rewriter.rollupColumnId(idx) -> expr.typ.canonical
    }.toMap
  }

  /** Analyze the query and map to column ids, just like we have in real life. */
  def analyzeQuery(q: String) = analyzer.analyzeFullQuery(q)(dsContext).mapColumnIds(columnIdMapping)

  /** Analyze a "fake" query that has the rollup table column names in, so we
    * can use it to compare  with the rewritten one in assertions.
    */
  def analyzeRewrittenQuery(rollupName: String, q: String) = {
    val rewrittenRawSchema = rollupRawSchemas(rollupName)

    val rollupNoopColumnNameMap = rewrittenRawSchema.map { case (k, v) => ColumnName(k) -> k}

    val rollupDsContext = QueryParser.dsContext(rollupNoopColumnNameMap, rewrittenRawSchema) match {
      case Right(ctx) => ctx
      case Left(unknownColumnIds) => throw new RuntimeException("Unknown column ids:" + unknownColumnIds)
    }

    val rewrittenQueryAnalysis = analyzer.analyzeFullQuery(q)(rollupDsContext).mapColumnIds(rollupNoopColumnNameMap)
    rewrittenQueryAnalysis
  }

  /** Silly half-assed function for debugging when things don't match */
  def compareProducts(a: Product, b: Product, indent: Int = 0): Unit = {
    val zip = a.productIterator.zip(b.productIterator)
    zip.foreach { case (x, y) =>
      println(">"*indent + "compare:" + (x == y) + " -- " + x + " == " +y )
      (x, y) match {
        case(xx: Product, yy: Product) => compareProducts(xx, yy, indent+1)
        case _ => println(">" * indent + s"Can't compare ${x} with ${y} but ... ${x.hashCode} vs ${y.hashCode}")
      }
    }
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
    rewrites should have size(2)
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


  ignore("count on literal - map query crime_type, count(0), count('potato')") {
    val q = "SELECT crime_type, count(0) as crimes, count('potato'), as crimes_potato GROUP BY crime_type"
    val queryAnalysis = analyzeQuery(q)

    val rewrittenQuery = "SELECT c2 AS crime_type, sum(c3) as crimes, sum(c3) as crimes_potato GROUP by c2"

    val rewrittenQueryAnalysis = analyzeRewrittenQuery("r4", rewrittenQuery)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key("r4")
    rewrites.get("r4").get  should equal(rewrittenQueryAnalysis)

    rewrites should have size(1)
  }


  // TODO count(coulumnname)
  // TODO count(literal)
  // TODO where
  // TODO order
  // TODO date functions
}


