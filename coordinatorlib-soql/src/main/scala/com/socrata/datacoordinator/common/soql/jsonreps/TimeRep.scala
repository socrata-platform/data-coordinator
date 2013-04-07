package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._
import org.joda.time.LocalTime

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLTime, SoQLType}
import com.socrata.soql.environment.ColumnName

class TimeRep(val name: ColumnName) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLTime

  private def tryParseTimestamp(s: String): Option[SoQLTime] = s match {
    case SoQLTime.StringRep(t) => Some(SoQLTime(t))
    case _ => None
  }

  private def printTimestamp(t: LocalTime): String =
    SoQLTime.StringRep(t)

  def fromJValue(input: JValue) = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLTime(time) => JString(printTimestamp(time))
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
