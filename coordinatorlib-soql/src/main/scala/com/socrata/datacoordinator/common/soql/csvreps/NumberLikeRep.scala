package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

class NumberLikeRep(val representedType: SoQLType, ctor: java.math.BigDecimal => SoQLValue) extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) {
      Some(SoQLNull)
    } else {
      try {
        Some(ctor(new java.math.BigDecimal(row(indices(0)))))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }
}
