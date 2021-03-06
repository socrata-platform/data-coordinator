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

  def estimateSize(v: SampleValue) = 16

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
