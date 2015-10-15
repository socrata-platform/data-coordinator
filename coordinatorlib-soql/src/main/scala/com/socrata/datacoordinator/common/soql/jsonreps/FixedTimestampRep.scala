package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast._
import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFixedTimestamp, SoQLType}
import com.socrata.soql.environment.ColumnName

object FixedTimestampRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLFixedTimestamp

  private def tryParseTimestamp(s: String): Option[SoQLFixedTimestamp] = s match {
    case SoQLFixedTimestamp.StringRep(dt) => Some(SoQLFixedTimestamp(dt))
    case _ => None
  }

  private def printTimestamp(t: DateTime): String =
    SoQLFixedTimestamp.StringRep(t)

  def fromJValue(input: JValue): Option[SoQLValue] = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue): JValue = input match {
    case SoQLFixedTimestamp(time) => JString(printTimestamp(time))
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
