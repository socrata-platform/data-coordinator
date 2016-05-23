package com.socrata.querycoordinator.fusion

import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.ast._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.parsing.standalone_exceptions.BadParse
import com.socrata.soql.types.SoQLType
import com.typesafe.scalalogging.slf4j.Logging

import scala.util.parsing.input.NoPosition

sealed trait FuseType

case object FTLocation extends FuseType
case object FTPhone extends FuseType
case object FTUrl extends FuseType
case object FTRemove extends FuseType

trait SoQLRewrite {
  def rewrite(select: Select): Select

  def postAnalyze(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]]): Seq[SoQLAnalysis[ColumnName, SoQLType]]
}

object CompoundTypeFuser {
  def apply(fuseBase: Map[String, String]): SoQLRewrite = {
    if (fuseBase.nonEmpty) new CompoundTypeFuser(fuseBase)
    else NoopFuser
  }
}

/**
 * This class translates compound type expressions base on expanded columns.
 *
 * Example:
 *
 * SELECT location
 *  WHERE location.latitude = 1.1
 *
 *  is rewritten as
 *
 * SELECT location(location,location_address,location_city,location_state,location_zip) AS location
 *  WHERE point_latitude(location) = 1.1
 *
 * @param fuseBase contains the base name of expanded columns which need to be fused.
 */
class CompoundTypeFuser(fuseBase: Map[String, String]) extends SoQLRewrite with Logging {

  private val ColumnPrefix = "#" // prefix should have no special meaning in regex.

  // Expand fuse map to include column sub-columns
  // for example, location will expand to include location_address, city, state, zip.
  private val fuse = fuseBase.foldLeft(Map.empty[String, FuseType]) { (acc, kv) =>
    kv match {
      case (name, "location") =>
        acc ++ Map(name -> FTLocation,
                   s"${name}_address" -> FTRemove,
                   s"${name}_city" -> FTRemove,
                   s"${name}_state" -> FTRemove,
                   s"${name}_zip" -> FTRemove)
      case (name, "phone") =>
        acc ++ Map(name -> FTPhone,
                   s"${name}_type" -> FTRemove)
      case (name, "url") =>
        acc ++ Map(name -> FTUrl,
                   s"${name}_description" -> FTRemove)
      case (name, typ) =>
        logger.warn(s"unrecognize fuse type {} for {}", typ, name)
        acc
    }
  }

  def rewrite(select: Select): Select = {
    val fusedSelectExprs = select.selection.expressions.flatMap {
      case SelectedExpression(expr: Expression, namePos) =>
        rewrite(expr) match {
          case Some(rwExpr) =>
            val alias =
              if (expr.eq(rwExpr)) { // Do not change the original name if the expression is not rewritten.
                namePos
              } else {
                namePos.orElse(Some((ColumnName(ColumnPrefix + expr.toSyntheticIdentifierBase), NoPosition)))
              }
            Seq(SelectedExpression(rwExpr, alias))
          case None =>
            Seq.empty
        }
    }

    val fusedWhere = select.where.map(e => rewrite(e).getOrElse(e))
    val fusedGroupBy = select.groupBy.map(gbs => gbs.flatMap(e => rewrite(e).toSeq))
    val fusedHaving = select.having.map(e => rewrite(e).getOrElse(e))
    val fusedOrderBy = select.orderBy.map(obs => obs.map(ob => ob.copy(expression = rewrite(ob.expression).get)))
    select.copy(
      selection = select.selection.copy(expressions = fusedSelectExprs),
      where = fusedWhere,
      groupBy = fusedGroupBy,
      having = fusedHaving,
      orderBy = fusedOrderBy
    )
  }

  /**
   * Columns involved are prefixed during ast rewrite and removed after analysis to avoid column name conflicts.
   */
  def postAnalyze(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]]): Seq[SoQLAnalysis[ColumnName, SoQLType]] = {
    val last = analyses.last
    val newSelect = last.selection map {
      case (cn, expr) =>
        (ColumnName(cn.name.replaceFirst(ColumnPrefix, "")) -> expr)
    }

    analyses.updated(analyses.size - 1, last.copy(selection = newSelect))
  }

  private def rewrite(expr: Expression): Option[Expression] = {
    expr match {
      case baseColumn@ColumnOrAliasRef(name: ColumnName) =>
        fuse.get(name.name) match {
          case Some(FTLocation) =>
            val address = ColumnOrAliasRef(ColumnName(s"${name.name}_address"))(NoPosition)
            val city = ColumnOrAliasRef(ColumnName(s"${name.name}_city"))(NoPosition)
            val state = ColumnOrAliasRef(ColumnName(s"${name.name}_state"))(NoPosition)
            val zip = ColumnOrAliasRef(ColumnName(s"${name.name}_zip"))(NoPosition)
            val args = Seq(baseColumn, address, city, state, zip)
            val fc = FunctionCall(SoQLFunctions.Location.name, args)(NoPosition, NoPosition)
            Some(fc)
          case Some(FTPhone) =>
            val phoneType = ColumnOrAliasRef(ColumnName(s"${name.name}_type"))(NoPosition)
            var args = Seq(baseColumn, phoneType)
            val fc = FunctionCall(SoQLFunctions.Phone.name, args)(NoPosition, NoPosition)
            Some(fc)
          case Some(FTUrl) =>
            val urlDescription = ColumnOrAliasRef(ColumnName(s"${name.name}_description"))(NoPosition)
            var args = Seq(baseColumn, urlDescription)
            val fc = FunctionCall(SoQLFunctions.Url.name, args)(NoPosition, NoPosition)
            Some(fc)
          case Some(FTRemove) =>
            None
          case None =>
            Some(expr)
        }
      case fc@FunctionCall(fnName, Seq(ColumnOrAliasRef(name: ColumnName)))
        if (Set(SoQLFunctions.PointToLatitude.name, SoQLFunctions.PointToLongitude.name).contains(fnName)) => Some(fc)
      case fc@FunctionCall(fnName, Seq(ColumnOrAliasRef(name: ColumnName), StringLiteral(prop)))
        if fnName.name == SpecialFunctions.Subscript.name =>
        fuse.get(name.name) match {
          case Some(FTLocation) =>
            prop match {
              case "latitude" =>
                val args = Seq(ColumnOrAliasRef(ColumnName(s"${name.name}"))(NoPosition))
                Some(FunctionCall(SoQLFunctions.PointToLatitude.name, args)(NoPosition, NoPosition))
              case "longitude" =>
                val args = Seq(ColumnOrAliasRef(ColumnName(s"${name.name}"))(NoPosition))
                Some(FunctionCall(SoQLFunctions.PointToLatitude.name, args)(NoPosition, NoPosition))
              case "human_address" =>
                val args = Seq("address", "city", "state", "zip")
                  .map(subProp => ColumnOrAliasRef(ColumnName(s"${name.name}_$subProp"))(NoPosition))
                Some(FunctionCall(SoQLFunctions.HumanAddress.name, args)(NoPosition, NoPosition))
              case _ =>
                throw BadParse("unknown location property", fc.position)
            }
          case Some(FTPhone) =>
            prop match {
              case "phone_number" =>
                Some(ColumnOrAliasRef(ColumnName(name.name))(NoPosition))
              case "phone_type" =>
                Some(ColumnOrAliasRef(ColumnName(s"${name.name}_type"))(NoPosition))
              case _ =>
                throw BadParse("unknown phone property", fc.position)
            }
          case Some(FTUrl) =>
            prop match {
              case "url" =>
                Some(ColumnOrAliasRef(ColumnName(name.name))(NoPosition))
              case "description" =>
                Some(ColumnOrAliasRef(ColumnName(s"${name.name}_description"))(NoPosition))
              case _ =>
                throw BadParse("unknown phone property", fc.position)
            }
          case Some(FTRemove) =>
            throw BadParse("subscript call on sub-column", fc.position)
          case _ =>
            Some(fc)
        }
      case fc@FunctionCall(fnName, params) =>
         val rwParams: Seq[Expression] = params.map(e => rewrite(e).getOrElse(e))
         Some(fc.copy(parameters = rwParams)(fc.functionNamePosition, fc.position))
      case _ =>
        Some(expr)
    }
  }
}
