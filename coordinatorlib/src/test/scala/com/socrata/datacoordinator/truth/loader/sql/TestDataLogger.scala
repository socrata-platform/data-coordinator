package com.socrata.datacoordinator
package truth.loader.sql

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.LinkedHashMap

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec

import com.socrata.datacoordinator.truth.loader.DataLogger
import com.socrata.datacoordinator.util.{RowUtils, Counter}
import com.rojoma.json.util.JsonUtil
import com.socrata.datacoordinator.id.{ColumnId, RowId}

class TestDataLogger(conn: Connection, logTableName: String, sidCol: ColumnId) extends DataLogger[TestColumnValue] {
  val subVersion = new Counter(init = 1)

  val list = new VectorBuilder[JValue]

  implicit val jCodec = new JsonCodec[TestColumnValue] {
    def encode(x: TestColumnValue) = x match {
      case StringValue(s) => JString(s)
      case LongValue(n) => JNumber(n)
      case NullValue => JNull
    }

    def decode(x: JValue) = x match {
      case JString(s) => Some(StringValue(s))
      case JNumber(n) => Some(LongValue(n.toLong))
      case JNull => Some(NullValue)
      case _ => None
    }
  }

  def sortRow(row: Row[TestColumnValue]) = {
    val r = new LinkedHashMap[String, TestColumnValue]
    row.toSeq.sortBy(_._1).foreach { case (k, v) =>
      r(k.underlying.toString) = v
    }
    r
  }

  def insert(systemID: RowId, row: Row[TestColumnValue]) {
    assert(row.get(sidCol) == Some(LongValue(systemID.underlying)))
    list += JObject(Map("i" -> JsonCodec.toJValue(sortRow(row))))
  }

  def update(sid: RowId, oldRow: Option[Row[TestColumnValue]], newRow: Row[TestColumnValue]) {
    assert(oldRow.isDefined, "We should never generate None for old-row")
    assert(oldRow.get.get(sidCol) == Some(LongValue(sid.underlying)))
    assert(newRow.get(sidCol) == Some(LongValue(sid.underlying)))
    val delta = RowUtils.delta(oldRow.get, newRow)
    list += JObject(Map("u" -> JsonCodec.toJValue(List(oldRow.get, delta).map(sortRow))))
  }

  def delete(systemID: RowId, oldRow: Option[Row[TestColumnValue]]) {
    assert(oldRow.isDefined, "We should never generate None for old-row")
    assert(oldRow.get.get(sidCol) == Some(LongValue(systemID.underlying)))
    list += JObject(Map("d" -> JsonCodec.toJValue(sortRow(oldRow.get))))
  }

  def counterUpdated(nextCtr: Long) {
    sys.error("Shouldn't call this")
  }

  def finish() {
    val ops = list.result()
    if(ops.nonEmpty) {
      using(conn.prepareStatement("INSERT INTO " + logTableName + " (version, subversion, rows, who) VALUES (1, ?, ?, 'hello')")) { stmt =>
        stmt.setLong(1, subVersion())
        stmt.setString(2, JsonUtil.renderJson(list.result()))
        stmt.executeUpdate()
      }
    }
  }

  def close() {}
}
