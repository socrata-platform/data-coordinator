package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast._
import org.joda.time.LocalDate

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLDate, SoQLType}
import com.socrata.soql.environment.ColumnName

object DateRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLDate

  private def tryParseTimestamp(s: String): Option[SoQLDate] = s match {
    case SoQLDate.StringRep(d) => Some(SoQLDate(d))
    case _ => None
  }

  private def printTimestamp(d: LocalDate): String =
    SoQLDate.StringRep(d)

  def fromJValue(input: JValue): Option[SoQLValue] = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue): JValue = input match {
    case SoQLDate(date) => JString(printTimestamp(date))
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
