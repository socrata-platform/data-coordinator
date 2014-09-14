package com.socrata.querycoordinator

import com.socrata.soql.types.{SoQLAnalysisType, SoQLType}
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.exceptions.SoQLException

import QueryParser._
import com.socrata.soql.SoQLAnalysis

class QueryParser(analyzer: SoQLAnalyzer[SoQLAnalysisType], maxRows: Option[Int], defaultRowsLimit: Int) {

  private def go(columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType])(f: DatasetContext[SoQLAnalysisType] => SoQLAnalysis[ColumnName, SoQLAnalysisType]): Result =
    dsContext(columnIdMapping, schema) match {
      case Right(ds) =>
        try {
          limitRows(f(ds)) match {
            case Right(analysis) => SuccessfulParse(analysis.mapColumnIds(columnIdMapping))
            case Left(result) => result
          }
        } catch {
          case e: SoQLException =>
            AnalysisError(e)
        }
      case Left(unknownColumnIds) =>
        UnknownColumnIds(unknownColumnIds)
    }

  private def limitRows(analysis: SoQLAnalysis[ColumnName, SoQLAnalysisType]): Either[Result, SoQLAnalysis[ColumnName, SoQLAnalysisType]] = {
    maxRows match {
      case Some(max) =>
        analysis.limit match {
          case Some(lim) if lim <= max => Right(analysis)
          case Some(lim) => Left(RowLimitExceeded(max)) // lim > max
          case None => Right(analysis.copy(limit = Some(defaultRowsLimit)))
        }
      case None => Right(analysis)
    }
  }

  def apply(query: String, columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType]): Result =
    go(columnIdMapping, schema)(analyzer.analyzeFullQuery(query)(_))

  def apply(selection: Option[String], where: Option[String], groupBy: Option[String], having: Option[String], orderBy: Option[String], limit: Option[String], offset: Option[String], search: Option[String], columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType]): Result =
    go(columnIdMapping, schema)(analyzer.analyzeSplitQuery(selection, where, groupBy, having, orderBy, limit, offset, search)(_))
}

object QueryParser {
  sealed abstract class Result
  case class SuccessfulParse(analysis: SoQLAnalysis[String, SoQLAnalysisType]) extends Result
  case class AnalysisError(problem: SoQLException) extends Result
  case class UnknownColumnIds(columnIds: Set[String]) extends Result
  case class RowLimitExceeded(max: Int) extends Result

  def dsContext(columnIdMapping: Map[ColumnName, String], rawSchema: Map[String, SoQLType]): Either[Set[String], DatasetContext[SoQLAnalysisType]] =
    try {
      val unknownColumnIds = columnIdMapping.values.iterator.filterNot(rawSchema.contains).toSet
      if (unknownColumnIds.isEmpty)
        Right(new DatasetContext[SoQLAnalysisType] {
          val schema: OrderedMap[ColumnName, SoQLAnalysisType] = OrderedMap(columnIdMapping.mapValues(rawSchema).toSeq.sortBy(_._1): _*)
        })
      else
        Left(unknownColumnIds)
    }
}
