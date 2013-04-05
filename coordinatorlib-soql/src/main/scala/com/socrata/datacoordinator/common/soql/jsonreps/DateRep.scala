package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._
import org.joda.time.LocalDate
import org.joda.time.format.ISODateTimeFormat

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLDate, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class DateRep(val name: ColumnName) extends JsonColumnRep[SoQLType, Any] {
  val representedType = SoQLDate

  private val formatter = ISODateTimeFormat.date
  private val parser = ISODateTimeFormat.dateElementParser

  private def tryParseTimestamp(s: String): Option[LocalDate] =
    try {
      Some(parser.parseLocalDate(s))
    } catch {
      case _: IllegalArgumentException =>
        None
    }

  private def printTimestamp(t: LocalDate): String =
    formatter.print(t)

  def fromJValue(input: JValue) = input match {
    case JString(s) => tryParseTimestamp(s)
    case JNull => Some(SoQLNullValue)
    case _ => None
  }

  def toJValue(input: Any) = input match {
    case time: LocalDate => JString(printTimestamp(time))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
