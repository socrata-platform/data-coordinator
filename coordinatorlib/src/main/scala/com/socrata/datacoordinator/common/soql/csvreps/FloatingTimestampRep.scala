package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFloatingTimestamp, SoQLType}
import org.joda.time.{LocalDateTime, DateTime}
import org.joda.time.format.{DateTimeFormatter, DateTimeParser, DateTimeFormat}

object FloatingTimestampRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLFloatingTimestamp

  val tsParser = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZoneUTC
  val alternateTsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC

  def decoder(formats: DateTimeFormatter*)(raw: String): Option[LocalDateTime] = {
    formats.foreach { format =>
      try {
        return Some(format.parseLocalDateTime(raw))
      } catch {
        case e: IllegalArgumentException => // ignore
      }
    }
    None
  }

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) {
      Some(SoQLNull)
    } else {
      decoder(tsParser, alternateTsParser)(x).map(SoQLFloatingTimestamp(_))
    }
  }
}
