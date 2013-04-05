package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLLocation, SoQLType}
import com.socrata.datacoordinator.common.soql.{SoQLValue, SoQLLocationValue, SoQLNullValue}

object LocationRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLLocation

  val fmt = """^\(([0-9.-]+), ([0-9.-]+)\)$""".r

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNullValue)
    else {
      fmt.findFirstMatchIn(x) map { mtch =>
        SoQLLocationValue(mtch.group(1).toDouble, mtch.group(2).toDouble)
      }
    }
  }
}
