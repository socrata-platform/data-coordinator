package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry

class GeometryLikeRep[T <: Geometry](repType: SoQLType, value: T => SoQLValue) extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1
  val representedType = repType

  def fromJson(str: String) = repType.asInstanceOf[SoQLGeometryLike[T]].JsonRep.unapply(str)

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    val x = row(indices(0))
    if(x.isEmpty) Some(SoQLNull)
    else fromJson(x) match {
      case Some(g) => Some(value(g))
      case _ => Some(SoQLNull)
    }
  }
}
