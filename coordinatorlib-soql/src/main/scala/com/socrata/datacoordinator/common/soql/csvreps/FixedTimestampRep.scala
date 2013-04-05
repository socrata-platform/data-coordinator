package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLType}
import org.joda.time.format.DateTimeFormat
import com.socrata.datacoordinator.common.soql.{SoQLFixedTimestampValue, SoQLValue, SoQLNullValue}

object FixedTimestampRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLFixedTimestamp

  val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNullValue)
    else try {
      Some(SoQLFixedTimestampValue(tsParser.parseDateTime(x)))
    } catch {
      case e: IllegalArgumentException =>
        None
    }
  }
}
