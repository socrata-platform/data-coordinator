package com.socrata.datacoordinator.common.soql.csvreps

import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry

class GeometryLikeRep[T <: Geometry](repType: SoQLType, value: T => SoQLValue) extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1
  val representedType = repType

  def fromWkt(str: String): Option[T] = repType.asInstanceOf[SoQLGeometryLike[T]].WktRep.unapply(str)

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[SoQLValue] = {
    assert(indices.size == size)
    val data = row(indices(0))
    if(data.isEmpty) {
      Some(SoQLNull)
    } else {
      fromWkt(data) match {
        case Some(g) => Some(value(g))
        case _ => Some(SoQLNull)
      }
    }
  }
}
