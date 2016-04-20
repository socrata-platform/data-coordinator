package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types._

object UrlRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 2

  val representedType = SoQLUrl

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)

    def optionalVal(s: String) = if (s.isEmpty) None else Some(s)

    val url = optionalVal(row(indices(0)))
    val description = optionalVal(row(indices(1)))
    if (url.isDefined || description.isDefined) {
      Some(SoQLUrl(url, description))
    } else {
      Some(SoQLNull)
    }
  }
}
