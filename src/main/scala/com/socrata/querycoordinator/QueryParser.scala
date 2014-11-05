package com.socrata.querycoordinator


import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType}
import com.typesafe.scalalogging.slf4j.Logging

class QueryParser(analyzer: SoQLAnalyzer[SoQLAnalysisType], maxRows: Option[Int], defaultRowsLimit: Int) {

  import QueryParser._

  private def go(columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType])
                (f: DatasetContext[SoQLAnalysisType] => SoQLAnalysis[ColumnName, SoQLAnalysisType])
                : Result = {
    val ds = dsContext(columnIdMapping, schema)
    try {
      limitRows(f(ds)) match {
        case Right(analysis) => SuccessfulParse(analysis.mapColumnIds(columnIdMapping))
        case Left(result) => result
      }
    } catch {
      case e: SoQLException =>
        AnalysisError(e)
    }
  }

  private def limitRows(analysis: SoQLAnalysis[ColumnName, SoQLAnalysisType]): Either[Result, SoQLAnalysis[ColumnName, SoQLAnalysisType]] = {
    analysis.limit match {
      case Some(lim) =>
        val actualMax = BigInt(maxRows.map(_.toLong).getOrElse(Long.MaxValue))
        if (lim <= actualMax) Right(analysis)
        else                  Left(RowLimitExceeded(actualMax))
      case None      =>
        Right(analysis.copy(limit = Some(defaultRowsLimit)))
    }
  }

  def apply(query: String, columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType]): Result =
    go(columnIdMapping, schema)(analyzer.analyzeFullQuery(query)(_))

  def apply(selection: Option[String], where: Option[String], groupBy: Option[String], having: Option[String], orderBy: Option[String], limit: Option[String], offset: Option[String], search: Option[String], columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType]): Result =
    go(columnIdMapping, schema)(analyzer.analyzeSplitQuery(selection, where, groupBy, having, orderBy, limit, offset, search)(_))
}

object QueryParser extends Logging {
  sealed abstract class Result
  case class SuccessfulParse(analysis: SoQLAnalysis[String, SoQLAnalysisType]) extends Result
  case class AnalysisError(problem: SoQLException) extends Result
  case class UnknownColumnIds(columnIds: Set[String]) extends Result
  case class RowLimitExceeded(max: BigInt) extends Result

  /**
   * Make schema which is a mapping of column name to datatype
   * by going through the raw schema of column id to datatype map.
   * Ignore columns that exist in truth but missing in secondary.
   * @param columnIdMapping column name to column id map (supposedly from soda fountain)
   * @param rawSchema column id to datatype map like ( xxxx-yyyy -> text, ... ) (supposedly from secondary)
   * @return column name to datatype map like ( field_name -> text, ... )
   */
  def dsContext(columnIdMapping: Map[ColumnName, String], rawSchema: Map[String, SoQLType]): DatasetContext[SoQLAnalysisType] =
    try {
      val knownColumnIdMapping = columnIdMapping.filter { case (k, v) => rawSchema.contains(v) }
      if (columnIdMapping.size != knownColumnIdMapping.size) {
        logger.info(s"truth has columns unknown to secondary ${columnIdMapping.size} ${knownColumnIdMapping.size}")
      }
      new DatasetContext[SoQLAnalysisType] {
        val schema: OrderedMap[ColumnName, SoQLAnalysisType] = OrderedMap(knownColumnIdMapping.mapValues(rawSchema).toSeq.sortBy(_._1): _*)
      }
    }
}
