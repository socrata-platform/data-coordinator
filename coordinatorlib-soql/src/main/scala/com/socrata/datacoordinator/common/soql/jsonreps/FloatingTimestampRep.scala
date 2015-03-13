package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast._
import org.joda.time.LocalDateTime

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFloatingTimestamp, SoQLType}
import com.socrata.soql.environment.ColumnName

object FloatingTimestampRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLFloatingTimestamp

  private def tryParseTimestamp(s: String): Option[SoQLFloatingTimestamp] = s match {
    case SoQLFloatingTimestamp.StringRep(dt) => Some(SoQLFloatingTimestamp(dt))
    case _ => None
  }

  private def printTimestamp(t: LocalDateTime): String =
    SoQLFloatingTimestamp.StringRep(t)

  def fromJValue(input: JValue) = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLFloatingTimestamp(time) => JString(printTimestamp(time))
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
