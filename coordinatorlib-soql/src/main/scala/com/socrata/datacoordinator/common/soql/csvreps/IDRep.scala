package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.datacoordinator.id.{RowIdProcessor, RowId}
import com.socrata.datacoordinator.common.soql.SoQLNullValue

class IDRep(rowIdProcessor: RowIdProcessor) extends CsvColumnRep[SoQLType, Any] {
  val size = 1

  val representedType = SoQLID

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNullValue)
    else try {
      Some(rowIdProcessor(row(indices(0))))
    } catch {
      case _: NumberFormatException =>
        None
    }
  }
}
