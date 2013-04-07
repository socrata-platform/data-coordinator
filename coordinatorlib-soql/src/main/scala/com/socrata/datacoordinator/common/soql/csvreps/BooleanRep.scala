package com.socrata.datacoordinator.common.soql
package csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLBoolean, SoQLType}

object BooleanRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLBoolean

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x == "true") Some(SoQLBoolean.canonicalTrue)
    else if(x == "false") Some(SoQLBoolean.canonicalFalse)
    else if(x == "") Some(SoQLNull)
    else None
  }
}
