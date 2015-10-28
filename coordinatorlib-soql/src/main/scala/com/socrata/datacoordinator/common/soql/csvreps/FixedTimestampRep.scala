package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFixedTimestamp, SoQLType}
import org.joda.time.format.DateTimeFormat

object FixedTimestampRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLFixedTimestamp

  val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) {
      Some(SoQLNull)
    } else {
      try {
        Some(SoQLFixedTimestamp(tsParser.parseDateTime(x)))
      } catch {
        case e: IllegalArgumentException => None
      }
    }
  }
}
