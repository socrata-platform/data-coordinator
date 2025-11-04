package com.socrata.datacoordinator.truth.metadata

import com.socrata.prettyprint.prelude._
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.id.{IndexId, IndexName}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.ast

sealed trait IndexInfoLike extends Product {
  val systemId: IndexId
  val name: IndexName
  val expressions: String
  val filter: Option[String]
}

case class UnanchoredIndexInfo(@JsonKey("sid") systemId: IndexId,
                               @JsonKey("name") name: IndexName,
                               @JsonKey("expressions") expressions: String,
                               @JsonKey("filter") filter: Option[String]) extends IndexInfoLike

object UnanchoredIndexInfo extends ((IndexId, IndexName, String, Option[String]) => UnanchoredIndexInfo) {
  override def toString = "UnanchoredIndexInfo"

  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredIndexInfo]
}

case class IndexInfo(systemId: IndexId, copyInfo: CopyInfo, name: IndexName, expressions: String, filter: Option[String]) extends IndexInfoLike {
  def unanchored: UnanchoredIndexInfo = UnanchoredIndexInfo(systemId, name, expressions, filter)

  def replaceFieldName(oldName: ColumnName, newName: ColumnName): IndexInfo = {
    val parser = new Parser(AbstractParser.defaultParameters)

    def replace(e: ast.Expression): ast.Expression = {
      e match {
        case cr@ast.ColumnOrAliasRef(None, n) if n == oldName =>
          ast.ColumnOrAliasRef(None, newName)(cr.position)
        case fc@ast.FunctionCall(name, params, filter, window) =>
          ast.FunctionCall(name, params.map(replace), filter.map(replace), window)(fc.position, fc.functionNamePosition)
        case cr: ast.ColumnOrAliasRef => cr
        case l: ast.Literal => l
        case h: ast.Hole => h
      }
    }

    val newExpressions =
      try {
        val old = parser.orderings(expressions)
        old.map { ob => ob.copy(expression = replace(ob.expression)).doc }
          .concatWith { (a: Doc[Nothing], b: Doc[Nothing]) => a ++ d"," ++ b }
          .layoutSmart(LayoutOptions(pageWidth = PageWidth.Unbounded))
          .toString
      } catch {
        case e: SoQLException => expressions
      }
    val newFilter = filter.map { f =>
      try {
        replace(parser.expression(f)).toCompactString
      } catch {
        case e: SoQLException => f
      }
    }
    copy(expressions = newExpressions, filter = newFilter)
  }
}
