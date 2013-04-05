package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class FixedTimestampRep(val name: ColumnName) extends JsonColumnRep[SoQLType, Any] {
  val representedType = SoQLFixedTimestamp

  private val formatter = ISODateTimeFormat.dateTime.withZoneUTC
  private val parser = ISODateTimeFormat.dateTimeParser.withZoneUTC

  private def tryParseTimestamp(s: String): Option[DateTime] =
    try {
      Some(parser.parseDateTime(s))
    } catch {
      case _: IllegalArgumentException =>
        None
    }

  private def printTimestamp(t: DateTime): String =
    formatter.print(t)

  def fromJValue(input: JValue) = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNullValue)
    case _ => None
  }

  def toJValue(input: Any) = input match {
    case time: DateTime => JString(printTimestamp(time))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
