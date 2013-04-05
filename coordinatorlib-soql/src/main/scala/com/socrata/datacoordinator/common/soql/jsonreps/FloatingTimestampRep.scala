package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._
import org.joda.time.LocalDateTime
import org.joda.time.format.ISODateTimeFormat

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLType}
import com.socrata.datacoordinator.common.soql.{SoQLFloatingTimestampValue, SoQLValue, SoQLNullValue}
import com.socrata.soql.environment.ColumnName

class FloatingTimestampRep(val name: ColumnName) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLFloatingTimestamp

  private val formatter = ISODateTimeFormat.dateTime
  private val parser = ISODateTimeFormat.localDateOptionalTimeParser

  private def tryParseTimestamp(s: String): Option[SoQLFloatingTimestampValue] =
    try {
      Some(SoQLFloatingTimestampValue(parser.parseLocalDateTime(s)))
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

  def toJValue(input: SoQLValue) = input match {
    case SoQLFloatingTimestampValue(time) => JString(printTimestamp(time))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
