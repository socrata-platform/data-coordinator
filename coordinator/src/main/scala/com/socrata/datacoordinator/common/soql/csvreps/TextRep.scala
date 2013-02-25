package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLText, SoQLID, SoQLType}
import com.socrata.datacoordinator.id.RowId

object TextRep extends CsvColumnRep[SoQLType, Any] {
  val size = 1

  val representedType = SoQLText

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    Some(row(indices(0)))
  }
}
