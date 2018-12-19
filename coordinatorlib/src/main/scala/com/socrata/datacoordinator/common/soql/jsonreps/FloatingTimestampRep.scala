package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast._
import org.joda.time.LocalDateTime

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFloatingTimestamp, SoQLType}
import com.socrata.datacoordinator.common.soql.csvreps.{FloatingTimestampRep => FloatingTimestampCsvRep}

object FloatingTimestampRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLFloatingTimestamp

  private def printTimestamp(t: LocalDateTime): String =
    SoQLFloatingTimestamp.StringRep(t)

  private val decoders = FloatingTimestampCsvRep.decoder(FloatingTimestampCsvRep.tsParser, FloatingTimestampCsvRep.alternateTsParser) _

  def fromJValue(input: JValue) = input match {
    case JString(s) => decoders(s).map(SoQLFloatingTimestamp(_))
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLFloatingTimestamp(time) => JString(printTimestamp(time))
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
