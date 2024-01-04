package com.socrata.datacoordinator.truth.sql
package sample

import java.sql.{Types, PreparedStatement, ResultSet}

class SamplePointColumnRep(val base: String) extends SampleRepUtils with SqlColumnRep[SampleType, SampleValue] {
  def representedType = SamplePointColumn

  val sqlTypes = Array("DOUBLE PRECISION", "DOUBLE PRECISION")
  val x = physCol("x")
  val y = physCol("y")
  val physColumns = Array(x, y)

  def csvifyForInsert(sb: java.lang.StringBuilder, v: SampleValue) {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SampleValue) = {
    v match {
      case SamplePoint(vx, vy) =>
        Seq(Some(vx.toString), Some(vy.toString))
      case SampleNull =>
        Seq(None, None)
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
