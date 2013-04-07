package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLID, SoQLType}

object IDRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLID

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNull)
    else try {
      Some(SoQLID(row(indices(0)).toLong))
    } catch {
      case _: NumberFormatException =>
        None
    }
  }
}
