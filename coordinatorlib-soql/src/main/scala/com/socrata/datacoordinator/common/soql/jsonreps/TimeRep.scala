package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._
import org.joda.time.LocalTime
import org.joda.time.format.ISODateTimeFormat

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLTime, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class TimeRep(val name: ColumnName) extends JsonColumnRep[SoQLType, Any] {
  val representedType = SoQLTime

  private val formatter = ISODateTimeFormat.time
  private val parser = ISODateTimeFormat.timeElementParser

  private def tryParseTimestamp(s: String): Option[LocalTime] =
    try {
      Some(parser.parseLocalTime(s))
    } catch {
      case _: IllegalArgumentException =>
        None
    }

  private def printTimestamp(t: LocalTime): String =
    formatter.print(t)

  def fromJValue(input: JValue) = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNullValue)
    case _ => None
  }

  def toJValue(input: Any) = input match {
    case time: LocalTime => JString(printTimestamp(time))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
