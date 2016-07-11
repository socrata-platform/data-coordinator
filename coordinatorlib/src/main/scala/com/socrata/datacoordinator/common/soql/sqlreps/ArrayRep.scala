package com.socrata.datacoordinator.common.soql.sqlreps

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLArray, SoQLValue, SoQLType}
import com.rojoma.json.v3.io.{JsonReader, CompactJsonWriter}
import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}
import com.rojoma.json.v3.ast.JArray

class ArrayRep (val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType = SoQLArray

  def string(v: SoQLValue): String = CompactJsonWriter.toString(v.asInstanceOf[SoQLArray].value)

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("TEXT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    if(SoQLNull == v) { /* pass */ }
    else csvescape(sb, string(v))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, string(v))
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else string(v).length //ick

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val v = rs.getString(start)
    if(v == null) SoQLNull
    else SoQLArray(JsonReader.fromString(v).asInstanceOf[JArray])
  }
}
