package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLType}
import org.joda.time.format.DateTimeFormat
import com.socrata.datacoordinator.common.soql.{SoQLFloatingTimestampValue, SoQLValue, SoQLNullValue}

object FloatingTimestampRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLFloatingTimestamp

  val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNullValue)
    else try {
      Some(SoQLFloatingTimestampValue(tsParser.parseLocalDateTime(x)))
    } catch {
      case e: IllegalArgumentException =>
        None
    }
  }
}
