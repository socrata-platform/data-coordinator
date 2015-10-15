package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types._
import org.joda.time.format.DateTimeFormat

object TimeRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLDate

  val tsParser = DateTimeFormat.forPattern("hh:mm aa").withZoneUTC

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) {
      Some(SoQLNull)
    } else {
      try {
        Some(SoQLTime(tsParser.parseLocalTime(x)))
      } catch {
        case e: IllegalArgumentException => None
      }
    }
  }
}
