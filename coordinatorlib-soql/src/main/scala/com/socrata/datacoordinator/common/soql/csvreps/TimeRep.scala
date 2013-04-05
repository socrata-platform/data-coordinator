package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLDate, SoQLType}
import org.joda.time.format.DateTimeFormat
import com.socrata.datacoordinator.common.soql.{SoQLTimeValue, SoQLValue, SoQLNullValue}

object TimeRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLDate

  val tsParser = DateTimeFormat.forPattern("hh:mm aa").withZoneUTC

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNullValue)
    else try {
      Some(SoQLTimeValue(tsParser.parseLocalTime(x)))
    } catch {
      case e: IllegalArgumentException =>
        None
    }
  }
}
