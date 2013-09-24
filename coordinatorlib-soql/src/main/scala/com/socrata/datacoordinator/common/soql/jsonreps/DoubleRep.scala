package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._
import com.socrata.soql.environment.ColumnName

object DoubleRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLDouble

  def fromJValue(input: JValue) = input match {
    case JNumber(n) => Some(SoQLDouble(n.doubleValue))
    case JNull => Some(SoQLNull)
    case JString(n) => try { Some(SoQLDouble(n.toDouble)) } catch { case e: NumberFormatException => None } // For NaN/Infinities
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLDouble(d) =>
      if(d.isInfinite || d.isNaN) JString(d.toString)
      else JNumber(d)
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
