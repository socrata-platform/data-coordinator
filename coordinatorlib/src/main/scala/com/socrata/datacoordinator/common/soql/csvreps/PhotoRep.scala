package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLValue, SoQLPhoto, SoQLType}

object PhotoRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLPhoto

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Some[SoQLPhoto] = {
    assert(indices.size == size)
    Some(SoQLPhoto(row(indices(0))))
  }
}
