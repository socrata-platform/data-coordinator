package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types._

object PhoneRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 2

  val representedType = SoQLPhone

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)

    def optionalVal(s: String) = if (s.isEmpty) None else Some(s)

    val phoneNumber = optionalVal(row(indices(0)))
    val phoneType = optionalVal(row(indices(1)))
    if (phoneNumber.isDefined || phoneType.isDefined) {
      Some(SoQLPhone(phoneNumber, phoneType))
    } else {
      Some(SoQLNull)
    }
  }
}
