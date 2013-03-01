package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLText, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue

class NumberLikeRep(val representedType: SoQLType) extends CsvColumnRep[SoQLType, Any] {
  val size = 1

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNullValue)
    else try {
      Some(BigDecimal(row(indices(0))))
    } catch {
      case _: NumberFormatException =>
        None
    }
  }
}
