package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLValue, SoQLBlob, SoQLType}

object BlobRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLBlob

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Some[SoQLBlob] = {
    assert(indices.size == size)
    Some(SoQLBlob(row(indices(0))))
  }
}
