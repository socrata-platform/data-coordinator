package com.socrata.datacoordinator.truth.sample

import java.sql.{ResultSet, PreparedStatement}

import com.socrata.datacoordinator.truth.SqlPKAbleColumnRep

class SampleTextColumnRep(val base: String) extends SqlPKAbleColumnRep[SampleType, SampleValue] {
  def representedType = SampleTextColumn

  val sqlTypes = Array("TEXT")
  val physColumns = Array(base)

  def templateForMultiLookup(n: Int) = {
    val sb = new StringBuilder
    sb.append("(lower(").append(base).append(") IN (?")
    var remaining = n - 1
    while(remaining > 0) {
      sb.append(",?")
      remaining -= 1
    }
    sb.append("))").toString
  }

  def prepareMultiLookup(stmt: PreparedStatement, v: SampleValue, n: Int): Int = {
    v match {
      case SampleText(text) =>
        stmt.setString(n, canonicalize(text))
        n + 1
      case _ =>
        sys.error("Illegal value for text column")
    }
  }

  val templateForSingleLookup =
    "(lower(" + base + ")=?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SampleValue, n: Int) = prepareMultiLookup(stmt, v, n)

  def csvifyForInsert(sb: java.lang.StringBuilder, v: SampleValue) {
    v match {
      case SampleText(text) => appendDoubling(sb, text, '"')
      case SampleNull => // do nothing
      case _ => sys.error("Illegal value for text column")
    }
  }

  def estimateInsertSize(v: SampleValue) = v match {
    case SampleText(text) => text.length
    case SampleNull => 0
    case _ => sys.error("Illegal value for text column")
  }

  def SETsForUpdate(sb: java.lang.StringBuilder, v: SampleValue) {
    sb.append(base).append("=")
    v match {
      case SampleText(text) => sqlizeStringLiteral(sb, text)
      case SampleNull => sb.append("NULL")
      case _ => sys.error("Illegal value for text column")
    }
  }

  def estimateUpdateSize(v: SampleValue) = estimateInsertSize(v) + base.length

  def sql_in(literals: Iterable[SampleValue]) = {
    val sb = new java.lang.StringBuilder("(lower(").append(base).append(") IN (")
    val it = literals.iterator
    var didOne = false
    while(it.hasNext) {
      if(didOne) sb.append(',')
      else didOne = true

      it.next() match {
        case SampleText(text) =>
          sqlizeStringLiteral(sb, canonicalize(text))
        case _ =>
          sys.error("Illegal value for text literal")
      }
    }
    sb.append("))")
    sb.toString
  }

  def sql_==(literal: SampleValue) =
    literal match {
      case SampleText(text) =>
        val sb = new java.lang.StringBuilder("(lower(").append(base).append(")=")
        sqlizeStringLiteral(sb, canonicalize(text))
        sb.append(')')
        sb.toString
      case _ =>
        sys.error("Illegal value for text literal")
    }

  def fromResultSet(rs: ResultSet, start: Int) = {
    val s = rs.getString(start)
    if(s == null) SampleNull
    else SampleText(s)
  }

  val equalityIndexExpression = "lower(" + base + ") text_pattern_ops"

  def canonicalize(s: String) = s.toLowerCase

  def sqlizeStringLiteral(sb: java.lang.StringBuilder, v: String) {
    appendDoubling(sb, v, '\'')
  }

  def appendDoubling(sb: java.lang.StringBuilder, v: String, quote: Char) {
    var i = 0
    sb.append(quote)
    while(i != v.length) {
      val c = v.charAt(i)
      if(c != 0) { // NULs aren't allowed in PostgreSQL text fields, so just quietly drop them if they get this far.
        if(c == quote) sb.append(quote)
        sb.append(c)
      }
      i += 1
    }
    sb.append(quote)
  }
}

