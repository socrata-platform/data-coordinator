package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryParser.{RowLimitExceeded, SuccessfulParse}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLText, SoQLType}
import com.typesafe.config.ConfigFactory

class RowsLimitTest extends TestBase {

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  test("max rows and default rows limit are respected") {
    val configMaxRowsAndLimit = new QueryCoordinatorConfig(ConfigFactory.load("reference-max-rows-and-limit.conf"), "com.socrata.query-coordinator")
    val qp = new QueryParser(analyzer, configMaxRowsAndLimit.maxRows, configMaxRowsAndLimit.defaultRowsLimit)
    val cols =  Map[ColumnName, String](ColumnName("c") -> "c")
    val schema = Map[String, SoQLType]("c" -> SoQLText)
    (qp.apply("select *", cols, schema) match {
      case SuccessfulParse(analysis) => analysis.limit
      case x =>
    }) should be (Some(20))

    (qp.apply("select * limit 200", cols, schema) match {
      case SuccessfulParse(analysis) => analysis.limit
      case x =>
    }) should be (Some(200))


    (qp.apply("select * limit 201", cols, schema) match {
      case x@RowLimitExceeded(_) => x
      case _ =>
    }) should be (RowLimitExceeded(200))

  }

  test("no max rows and default rows limit") {
    val configMaxRowsAndLimit = new QueryCoordinatorConfig(ConfigFactory.load("reference-no-max-rows.conf"), "com.socrata.query-coordinator")
    val qp = new QueryParser(analyzer, configMaxRowsAndLimit.maxRows, configMaxRowsAndLimit.defaultRowsLimit)
    val cols =  Map[ColumnName, String](ColumnName("c") -> "c")
    val schema = Map[String, SoQLType]("c" -> SoQLText)
    (qp.apply("select *", cols, schema) match {
      case SuccessfulParse(analysis) => analysis.limit
      case x =>
    }) should be (None) // no limits

    (qp.apply("select * limit 100000", cols, schema) match {
      case SuccessfulParse(analysis) => analysis.limit
      case x =>
    }) should be (Some(100000))
  }
}

