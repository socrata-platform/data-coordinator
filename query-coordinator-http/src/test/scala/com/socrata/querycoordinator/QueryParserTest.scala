package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryParser.{AnalysisError, SuccessfulParse}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.types.{SoQLText, SoQLType}

class QueryParserTest extends TestBase {

  import QueryParserTest._

  test("SELECT * expands all columns") {
    val query = "select *"
    val starPos = query.indexOf("*") + 1
    val expected = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef("ai", SoQLText)(new SoQLPosition(1, starPos, query, 0)),
      ColumnName("b") -> ColumnRef("bi", SoQLText)(new SoQLPosition(1, starPos, query, 0))
    )
    val actual = qp.apply(query, truthColumns, upToDateSchema) match {
      case SuccessfulParse(analysis) => analysis.selection
      case x => x
    }
    actual should be (expected)
  }

  test("SELECT * ignores missing columns") {
    val query = "select *"
    val starPos = query.indexOf("*") + 1
    val expected = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef("ai", SoQLText)(new SoQLPosition(1, starPos, query, 0))
    )
    val actual = qp.apply(query, truthColumns, outdatedSchema) match {
      case SuccessfulParse(analysis) => analysis.selection
      case x => x
    }
    actual should be (expected)
  }

  test("SELECT non existing column errs") {
    val query = "select b"
    val actual = qp.apply(query, truthColumns, outdatedSchema)
    actual.getClass should be (classOf[AnalysisError])
  }
}

object QueryParserTest {

  val defaultRowLimit = 20

  val maxRowLimit = 200

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val qp = new QueryParser(analyzer, Some(maxRowLimit), defaultRowLimit)

  val truthColumns =  Map[ColumnName, String](ColumnName("a") -> "ai", ColumnName("b") -> "bi")

  val upToDateSchema = Map[String, SoQLType]("ai" -> SoQLText, "bi" -> SoQLText)

  val outdatedSchema = Map[String, SoQLType]("ai" -> SoQLText) // Does not have "column bi"

}