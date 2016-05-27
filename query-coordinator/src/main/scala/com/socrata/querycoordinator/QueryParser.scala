package com.socrata.querycoordinator

import com.socrata.querycoordinator.fusion.{CompoundTypeFuser, NoopFuser, SoQLRewrite}
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.ast.{Expression, Select, Selection}
import com.socrata.soql.collection.{OrderedMap, OrderedSet}
import com.socrata.soql.environment.{ColumnName, DatasetContext, UntypedDatasetContext}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.parsing.Parser
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.typesafe.scalalogging.slf4j.Logging

class QueryParser(analyzer: SoQLAnalyzer[SoQLType], maxRows: Option[Int], defaultRowsLimit: Int) {
  import QueryParser._ // scalastyle:ignore import.grouping

  private def go(columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType])
                (f: DatasetContext[SoQLType] => Seq[SoQLAnalysis[ColumnName, SoQLType]]): Result = {
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
                            analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]])
    : Seq[SoQLAnalysis[String, SoQLType]] = {
    val initialAcc = (columnIdMapping, Seq.empty[SoQLAnalysis[String, SoQLType]])
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

      val a: SoQLAnalysis[String, SoQLType] = analysis.mapColumnIds(newMapping)
      (mappingWithNewColumns, convertedAnalyses :+ a)
    }
    analysesInColIds
  }

  private def limitRows(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]])
    : Either[Result, Seq[SoQLAnalysis[ColumnName, SoQLType]]] = {
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

  private def analyzeQuery(query: String): DatasetContext[SoQLType] => Seq[SoQLAnalysis[ColumnName, SoQLType]] = {
      analyzer.analyzeFullQuery(query)(_)
  }

  private def analyzeQueryWithCompoundTypeFusion(query: String,
                                                 fuser: SoQLRewrite,
                                                 columnIdMapping: Map[ColumnName, String],
                                                 schema: Map[String, SoQLType]):
    DatasetContext[SoQLType] => Seq[SoQLAnalysis[ColumnName, SoQLType]] = {

    val parsedStmts = new Parser().selectStatement(query)

    // expand "select *"
    val firstStmt = parsedStmts.head
    val baseCtx = new UntypedDatasetContext {
      override val columns: OrderedSet[ColumnName] = {
        // exclude non-existing columns in the schema
        val existedColumns = columnIdMapping.filter { case (k, v) => schema.contains(v) }
        OrderedSet(existedColumns.keysIterator.toSeq: _*)
      }
    }

    val expandedStmts = parsedStmts.foldLeft((Seq.empty[Select], baseCtx)) { (acc, select) =>
      val (selects, ctx) = acc
      val expandedSelection = AliasAnalysis.expandSelection(select.selection)(ctx)
      val expandedStmt = select.copy(selection = Selection(None, None, expandedSelection))
      val columnNames = expandedStmt.selection.expressions.map { se =>
        se.name.map(_._1).getOrElse(ColumnName(se.expression.toString.replaceAllLiterally("`", "")))
      }
      val nextCtx = new UntypedDatasetContext {
        override val columns: OrderedSet[ColumnName] = OrderedSet(columnNames: _*)
      }
      (selects :+ expandedStmt, nextCtx)
    }._1

    // rewrite only the last statement.
    val lastExpandedStmt = expandedStmts.last
    val fusedStmts = expandedStmts.updated(expandedStmts.indexOf(lastExpandedStmt), fuser.rewrite(lastExpandedStmt))

    analyzer.analyze(fusedStmts)(_)
  }

  def apply(query: String,
            columnIdMapping: Map[ColumnName, String],
            schema: Map[String, SoQLType],
            fuseMap: Map[String, String] = Map.empty,
            merged: Boolean = true): Result = {

    val postAnalyze = CompoundTypeFuser(fuseMap) match {
      case x if x.eq(NoopFuser) =>
        analyzeQuery(query)
      case fuser: SoQLRewrite =>
        val analyze = analyzeQueryWithCompoundTypeFusion(query, fuser, columnIdMapping, schema)
        analyze andThen fuser.postAnalyze
    }

    val analyzeMaybeMerge = if (merged) { postAnalyze andThen soqlMerge } else { postAnalyze }
    go(columnIdMapping, schema)(analyzeMaybeMerge)
  }

  private def soqlMerge(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]])
    : Seq[SoQLAnalysis[ColumnName, SoQLType]] = {
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
            schema: Map[String, SoQLType],
            fuseMap: Map[String, String]): Result = {
    val query = fullQuery(selection, where, groupBy, having, orderBy, limit, offset, search)
    apply(query, columnIdMapping, schema, fuseMap)
  }
}

object QueryParser extends Logging {

  sealed abstract class Result

  case class SuccessfulParse(analyses: Seq[SoQLAnalysis[String, SoQLType]]) extends Result

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
                rawSchema: Map[String, SoQLType]): DatasetContext[SoQLType] =
    try {
      val knownColumnIdMapping = columnIdMapping.filter { case (k, v) => rawSchema.contains(v) }
      if (columnIdMapping.size != knownColumnIdMapping.size) {
        logger.warn(s"truth has columns unknown to secondary ${columnIdMapping.size} ${knownColumnIdMapping.size}")
      }
      new DatasetContext[SoQLType] {
        val schema: OrderedMap[ColumnName, SoQLType] =
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
  private val andFn = SoQLFunctions.And.monomorphic.get
}
