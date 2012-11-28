package com.socrata.datacoordinator.truth.sample

import java.sql.ResultSet

import com.socrata.datacoordinator.truth.SqlColumnRep

class SamplePointColumnRep(val base: String) extends SqlColumnRep[SampleType, SampleValue] {
  def representedType = SamplePointColumn

  val sqlTypes = Array("DOUBLE PRECISION", "DOUBLE PRECISION")
  val x = physCol("x")
  val y = physCol("y")
  val physColumns = Array(x, y)

  def csvifyForInsert(sb: java.lang.StringBuilder, v: SampleValue) {
    v match {
      case SamplePoint(vx, vy) =>
        sb.append(vx).append(",").append(vy)
      case SampleNull =>
        sb.append(",")
      case _ =>
        sys.error("Illegal value for point column")
    }
  }

  def estimateInsertSize(v: SampleValue) = 16

  def SETsForUpdate(sb: java.lang.StringBuilder, v: SampleValue) {
    v match {
      case SamplePoint(px, py) =>
        sb.append(x).append('=').append(px).append(',').append(y).append('=').append(py)
      case SampleNull =>
        sb.append(x).append("=NULL,").append(y).append("=NULL")
      case _ =>
        sys.error("Illegal value for point column")
    }
  }

  def estimateUpdateSize(v: SampleValue) = x.length + y.length + estimateInsertSize(v)

  def fromResultSet(rs: ResultSet, start: Int) = {
    val x = rs.getDouble(start)
    if(rs.wasNull()) SampleNull
    else {
      val y = rs.getDouble(start + 1)
      // either both are null, or neither, so no need to consult wasNull here.
      SamplePoint(x, y)
    }
  }
}
