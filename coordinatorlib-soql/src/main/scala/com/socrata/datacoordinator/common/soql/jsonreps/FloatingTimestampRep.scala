package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._
import org.joda.time.LocalDateTime
import org.joda.time.format.ISODateTimeFormat

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class FloatingTimestampRep(val name: ColumnName) extends JsonColumnRep[SoQLType, Any] {
  val representedType = SoQLFloatingTimestamp

  private val formatter = ISODateTimeFormat.dateTime.withZoneUTC
  private val parser = ISODateTimeFormat.dateTimeParser

  private def tryParseTimestamp(s: String): Option[LocalDateTime] =
    try {
      Some(parser.parseLocalDateTime(s))
    } catch {
      case _: IllegalArgumentException =>
        None
    }

  private def printTimestamp(t: LocalDateTime): String =
    formatter.print(t)

  def fromJValue(input: JValue) = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNullValue)
    case _ => None
  }

  def toJValue(input: Any) = input match {
    case time: LocalDateTime => JString(printTimestamp(time))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
