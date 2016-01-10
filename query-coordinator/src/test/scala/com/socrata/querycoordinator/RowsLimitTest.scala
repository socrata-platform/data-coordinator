package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryParser.{RowLimitExceeded, SuccessfulParse}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLText, SoQLType}

class RowsLimitTest extends TestBase {
  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  test("max rows and default rows limit are respected") {
    val defaultRowLimit = 20
    val maxRowLimit = 200

    val qp = new QueryParser(analyzer, Some(maxRowLimit), defaultRowLimit)
    val cols = Map[ColumnName, String](ColumnName("c") -> "c")
    val schema = Map[String, SoQLType]("c" -> SoQLText)
    (qp.apply("select *", cols, schema) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(defaultRowLimit))

    (qp.apply(s"select * limit $maxRowLimit", cols, schema) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(maxRowLimit))


    (qp.apply(s"select * limit ${maxRowLimit + 1}", cols, schema) match {
      case x@RowLimitExceeded(_) => x
      case _ =>
    }) should be(RowLimitExceeded(maxRowLimit))

  }

  test("max rows not configured") {
    val defaultRowLimit = 20

    val qp = new QueryParser(analyzer, None, defaultRowLimit)
    val cols = Map[ColumnName, String](ColumnName("c") -> "c")
    val schema = Map[String, SoQLType]("c" -> SoQLText)
    (qp.apply("select *", cols, schema) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(defaultRowLimit))

    (qp.apply("select * limit 100000", cols, schema) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(100000))
  }
}
