package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLValue, SoQLText, SoQLType}

object TextRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLText

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    Some(SoQLText(row(indices(0))))
  }
}
