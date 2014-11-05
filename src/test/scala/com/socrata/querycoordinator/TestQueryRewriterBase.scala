package com.socrata.querycoordinator

import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.SoQLType

class TestQueryRewriterBase extends TestBase {
  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter = new QueryRewriter(analyzer)

  /** The raw of the table that we get as part of the secondary /schema call */
  val rawSchema = Map[String, SoQLType](
    "dxyz-num1" -> SoQLType.typesByName(TypeName("number")),
    ":wido-ward" -> SoQLType.typesByName(TypeName("number")),
    "crim-typ3" -> SoQLType.typesByName(TypeName("text")),
    "dont-roll" -> SoQLType.typesByName(TypeName("text")),
    "crim-date" -> SoQLType.typesByName(TypeName("floating_timestamp")),
    "some-date" -> SoQLType.typesByName(TypeName("floating_timestamp"))
  )

  /** QueryRewriter wants a Schema object to have a little stronger typing, so make one */
  val schema = Schema("NOHASH", rawSchema, "NOPK")

  /** Mapping from column name to column id, that we get from soda fountain with the query.  */
  val columnIdMapping = Map[ColumnName, rewriter.ColumnId](
    ColumnName("number1") -> "dxyz-num1",
    ColumnName("ward") -> ":wido-ward",
    ColumnName("crime_type") -> "crim-typ3",
    ColumnName("dont_create_rollups") -> "dont-roll",
    ColumnName("crime_date") -> "crim-date",
    ColumnName("some_date") -> "some-date"
  )

  /** The dataset context, used for parsing the query */
  val dsContext = QueryParser.dsContext(columnIdMapping, rawSchema)

  /** Analyze the query and map to column ids, just like we have in real life. */
  def analyzeQuery(q: String) = analyzer.analyzeFullQuery(q)(dsContext).mapColumnIds(columnIdMapping)

  /** Silly half-assed function for debugging when things don't match */
  def compareProducts(a: Product, b: Product, indent: Int = 0): Unit = {
    val zip = a.productIterator.zip(b.productIterator)
    zip.foreach { case (x, y) =>
      println(">" * indent + "compare:" + (x == y) + " -- " + x + " == " + y)
      (x, y) match {
        case (xx: Product, yy: Product) => compareProducts(xx, yy, indent + 1)
        case _ => println(">" * indent + s"Can't compare ${x} with ${y} but ... ${x.hashCode} vs ${y.hashCode}")
      }
    }
  }
}
