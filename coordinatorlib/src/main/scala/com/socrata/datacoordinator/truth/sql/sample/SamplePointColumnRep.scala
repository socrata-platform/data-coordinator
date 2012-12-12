package com.socrata.datacoordinator.truth.sql
package sample

import java.sql.{Types, PreparedStatement, ResultSet}

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

  def prepareInsert(stmt: PreparedStatement, v: SampleValue, i: Int) = {
    v match {
      case SamplePoint(vx, vy) =>
        stmt.setDouble(i, vx)
        stmt.setDouble(i + 1, vy)
        i + 2
      case SampleNull =>
        stmt.setNull(i, Types.DOUBLE)
        stmt.setNull(i + 1, Types.DOUBLE)
        i + 2
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
