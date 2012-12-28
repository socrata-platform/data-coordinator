package com.socrata.datacoordinator
package truth.loader.sql

import scala.collection.immutable.{SortedMap, VectorBuilder}

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec

import com.socrata.datacoordinator.truth.loader.DataLogger
import com.socrata.datacoordinator.util.Counter
import com.rojoma.json.util.JsonUtil
import com.socrata.datacoordinator.id.RowId

class TestDataLogger(conn: Connection, logTableName: String) extends DataLogger[TestColumnValue] {
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

  def insert(systemID: RowId, row: Row[TestColumnValue]) {
    list += JObject(Map("i" -> JsonCodec.toJValue(SortedMap(row.toSeq : _*).map { kv => kv._1.underlying.toString -> kv._2 })))
  }

  def update(sid: RowId, row: Row[TestColumnValue]) {
    list += JObject(Map("u" -> JsonCodec.toJValue(SortedMap(row.toSeq : _*).map { kv => kv._1.underlying.toString -> kv._2 })))
  }

  def delete(systemID: RowId) {
    list += JObject(Map("d" -> JNumber(systemID.underlying)))
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
