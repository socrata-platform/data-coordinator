package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFloatingTimestamp, SoQLType}
import org.joda.time.format.DateTimeFormat

object FloatingTimestampRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLFloatingTimestamp

  val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNull)
    else try {
      Some(SoQLFloatingTimestamp(tsParser.parseLocalDateTime(x)))
    } catch {
      case e: IllegalArgumentException =>
        None
    }
  }
}
