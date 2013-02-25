package com.socrata.datacoordinator.common.soql
package csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLBoolean, SoQLType}

object BooleanRep extends CsvColumnRep[SoQLType, Any] {
  val size = 1

  val representedType = SoQLBoolean

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x == "true") Some(true)
    else if(x == "false") Some(false)
    else if(x == "") Some(SoQLNullValue)
    else None
  }
}
