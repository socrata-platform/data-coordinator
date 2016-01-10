package com.socrata.querycoordinator

import com.socrata.soql.ast.Expression
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.{MonomorphicFunction, VariableType, SoQLFunctions}
import com.socrata.soql.types.{SoQLBoolean, SoQLAnalysisType, SoQLType}
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.typesafe.scalalogging.slf4j.Logging

class QueryParser(analyzer: SoQLAnalyzer[SoQLAnalysisType], maxRows: Option[Int], defaultRowsLimit: Int) {
  import QueryParser._ // scalastyle:ignore import.grouping

  private def go(columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType])
                (f: DatasetContext[SoQLAnalysisType] => Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]]): Result = {
    val ds = dsContext(columnIdMapping, schema)
    try {
      limitRows(f(ds)) match {
        case Right(analyses) =>
          val analysesInColumnIds = remapAnalyses(columnIdMapping, analyses)
          SuccessfulParse(analysesInColumnIds)
        case Left(result) => result
      }
    } catch {
      case e: SoQLException =>
        AnalysisError(e)
    }
  }

  /**
   * Convert analyses from column names to column ids.
   * Add new columns and remap "column ids" as it walks the soql chain.
   */
  private def remapAnalyses(columnIdMapping: Map[ColumnName, String], //schema: Map[String, SoQLType],
                            analyses: Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]])
    : Seq[SoQLAnalysis[String, SoQLAnalysisType]] = {
    val initialAcc = (columnIdMapping, Seq.empty[SoQLAnalysis[String, SoQLAnalysisType]])
    val (_, analysesInColIds) = analyses.foldLeft(initialAcc) { (acc, analysis) =>
      val (mapping, convertedAnalyses) = acc
      // Newly introduced columns will be used as column id as is.
      // There should be some sanitizer upstream that checks for field_name conformity.
      // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
      val newlyIntroducedColumns = analysis.selection.keys.filter { columnName => !mapping.contains(columnName) }
      val mappingWithNewColumns = newlyIntroducedColumns.foldLeft(mapping) { (acc, newColumn) =>
        acc + (newColumn -> newColumn.name)
      }
      // Re-map columns except for the innermost soql
      val newMapping =
        if (convertedAnalyses.nonEmpty) {
          val prevAnalysis = convertedAnalyses.last
          prevAnalysis.selection.foldLeft(mapping) { (acc, selCol) =>
            val (colName, expr) = selCol
            acc + (colName -> colName.name)
          }
        } else {
          mappingWithNewColumns
        }

      val a: SoQLAnalysis[String, SoQLAnalysisType] = analysis.mapColumnIds(newMapping)
      (mappingWithNewColumns, convertedAnalyses :+ a)
    }
    analysesInColIds
  }

  private def limitRows(analyses: Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]])
    : Either[Result, Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]]] = {
    val lastAnalysis = analyses.last
    lastAnalysis.limit match {
      case Some(lim) =>
        val actualMax = BigInt(maxRows.map(_.toLong).getOrElse(Long.MaxValue))
        if (lim <= actualMax) { Right(analyses) }
        else { Left(RowLimitExceeded(actualMax)) }
      case None =>
        Right(analyses.dropRight(1) :+ lastAnalysis.copy(limit = Some(defaultRowsLimit)))
    }
  }

  def apply(query: String,
            columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType],
            merged: Boolean = true): Result = {
    val analyze: DatasetContext[SoQLAnalysisType] => Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]] =
      analyzer.analyzeFullQuery(query)(_)
    val analyzeMaybeMerge = if (merged) { analyze andThen soqlMerge } else { analyze }
    go(columnIdMapping, schema)(analyzeMaybeMerge)
  }

  private def soqlMerge(analyses: Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]])
    : Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]] = {
    SoQLAnalysis.merge(andFn, analyses)
  }

  def apply(selection: Option[String], // scalastyle:ignore parameter.number
            where: Option[String],
            groupBy: Option[String],
            having: Option[String],
            orderBy: Option[String],
            limit: Option[String],
            offset: Option[String],
            search: Option[String],
            columnIdMapping: Map[ColumnName, String],
            schema: Map[String, SoQLType]): Result = {
    val query = fullQuery(selection, where, groupBy, having, orderBy, limit, offset, search)
    go(columnIdMapping, schema)(analyzer.analyzeFullQuery(query)(_))
  }
}

object QueryParser extends Logging {

  sealed abstract class Result

  case class SuccessfulParse(analyses: Seq[SoQLAnalysis[String, SoQLAnalysisType]]) extends Result

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
  def dsContext(columnIdMapping: Map[ColumnName, String],
                rawSchema: Map[String, SoQLType]): DatasetContext[SoQLAnalysisType] =
    try {
      val knownColumnIdMapping = columnIdMapping.filter { case (k, v) => rawSchema.contains(v) }
      if (columnIdMapping.size != knownColumnIdMapping.size) {
        logger.info(s"truth has columns unknown to secondary ${columnIdMapping.size} ${knownColumnIdMapping.size}")
      }
      new DatasetContext[SoQLAnalysisType] {
        val schema: OrderedMap[ColumnName, SoQLAnalysisType] =
          OrderedMap(knownColumnIdMapping.mapValues(rawSchema).toSeq.sortBy(_._1): _*)
      }
    }

  private def fullQuery(selection : Option[String],
                        where : Option[String],
                        groupBy : Option[String],
                        having : Option[String],
                        orderBy : Option[String],
                        limit : Option[String],
                        offset : Option[String],
                        search : Option[String]): String = {
    val sb = new StringBuilder
    sb.append(selection.map( "SELECT " + _).getOrElse("SELECT *"))
    sb.append(where.map(" WHERE " + _).getOrElse(""))
    sb.append(where.map(" GROUP BY " + _).getOrElse(""))
    sb.append(where.map(" HAVING " + _).getOrElse(""))
    sb.append(where.map(" ORDER BY " + _).getOrElse(""))
    sb.append(where.map(" LIMIT " + _).getOrElse(""))
    sb.append(where.map(" OFFSET " + _).getOrElse(""))
    sb.append(where.map(s => "SEARCH %s".format(Expression.escapeString(s))).getOrElse(""))
    sb.result()
  }

  // And function is used for chain SoQL merge.
  private val andFnBindings = SoQLFunctions.Neq.parameters.map {
    case VariableType(name) => name -> SoQLBoolean
    case _ => throw new Exception("Unexpected function signature")
  }.toMap

  private val andFn = MonomorphicFunction(SoQLFunctions.Neq, andFnBindings)
}
