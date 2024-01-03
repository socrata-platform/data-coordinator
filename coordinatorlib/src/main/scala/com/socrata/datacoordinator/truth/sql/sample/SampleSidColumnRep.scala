package com.socrata.datacoordinator.truth.sql
package sample

import java.sql.{ResultSet, PreparedStatement}

class SampleSidColumnRep(val base: String) extends SampleRepUtils with SqlPKableColumnRep[SampleType, SampleValue] {
  def representedType = SampleSidColumn

  val sqlTypes = Array("BIGINT")
  val physColumns = Array(base)

  def templateForMultiLookup(n: Int) = {
    val sb = new StringBuilder
    sb.append('(').append(base).append(" IN (?")
    var remaining = n - 1
    while(remaining > 0) {
      sb.append(",?")
      remaining -= 1
    }
    sb.append("))").toString
  }

  def prepareMultiLookup(stmt: PreparedStatement, v: SampleValue, start: Int): Int = {
    stmt.setLong(start, extract(v))
    start + 1
  }

  val templateForSingleLookup =
    "(" + base + "=?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SampleValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def csvifyForInsert(sb: java.lang.StringBuilder, v: SampleValue) {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SampleValue) = {
    Seq(Some(extract(v).toString))
  }

  def prepareInsert(stmt: PreparedStatement, v: SampleValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def estimateSize(v: SampleValue) = 10

  def sql_in(literals: Iterable[SampleValue]) = {
    literals.iterator.map(extract).mkString("(" + base + " IN (", ",", "))")
  }

  def count = "count(" + base + ")"

  def sql_==(literal: SampleValue) = "(" + base + '=' + extract(literal) + ')'

  def fromResultSet(rs: ResultSet, start: Int): SampleValue =
    SampleSid(rs.getLong(start))

  def equalityIndexExpression = base

  def extract(v: SampleValue): Long = {
    v match {
      case SampleSid(id) => id
      case _ => sys.error("Illegal value for sid column")
    }
  }
}

