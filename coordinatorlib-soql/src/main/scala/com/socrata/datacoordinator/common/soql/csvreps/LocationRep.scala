package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types._

object LocationRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 2

  val representedType = SoQLLocation

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)
    val pointWkt: String = row(indices(0))
    val address: String = row(indices(1))
    if (pointWkt.isEmpty && address.isEmpty) {
      Some(SoQLNull)
    } else {
      val loc = for {ptwkt <- Option(pointWkt)
                     pt <- SoQLPoint.WktRep.unapply(pointWkt)
                } yield {
                  SoQLLocation(Some(java.math.BigDecimal.valueOf(pt.getY)),
                               Some(java.math.BigDecimal.valueOf(pt.getX)),
                               Option(address))
                }
      loc.orElse(Some(SoQLLocation(None, None, Option(address))))
    }
  }
}
