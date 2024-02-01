package com.socrata.datacoordinator.common

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.common.soql.{SoQLRep, SoQLRowLogCodec}
import com.socrata.datacoordinator.truth.loader.{Delete, Update, Insert, Delogger}
import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.loader.Delogger.RowDataUpdated
import com.socrata.datacoordinator.Row
import com.rojoma.json.v3.ast._
import scala.collection.immutable.SortedMap

object LogViewer extends App {
  val cfg = new DataSourceConfig(ConfigFactory.load(), "com.socrata.coordinator.utils")
  val table = args(0)
  val from = args(1).toLong
  val to = args(2).toLong

  trait Rep[T <: SoQLValue] extends CJsonWriteRep[T] with AsErasedCJsonWriteRep[T, SoQLValue] {
    def isNull(v: SoQLValue) = v == SoQLNull
    def mkNull = SoQLNull
  }

  val idRep = new Rep[SoQLID] {
    def toJValue(value: SoQLID): JValue = {
      JString("sid-" + value.value)
    }
    def downcast(v: SoQLValue) =
      v match {
        case id: SoQLID => Some(id)
        case _ => None
      }
  }
  val versionRep = new Rep[SoQLVersion] {
    def toJValue(value: SoQLVersion): JValue = {
      JString("ver-" + value.value)
    }
    def downcast(v: SoQLValue) =
      v match {
        case id: SoQLVersion => Some(id)
        case _ => None
      }
  }
  val jsonReps = SoQLRep.jsonRepsMinusIdAndVersion ++ Map(
    SoQLID -> idRep.asErasedCJsonWriteRep,
    SoQLVersion -> versionRep.asErasedCJsonWriteRep
  )

  def jsonify(row: Row[SoQLValue]): JObject =
    JObject(SortedMap[String,JObject]() ++ row.iterator.map { case (cid, v) =>
      cid.underlying.toString -> jsonReps(v.typ).toJValue(v)
    })

  def dumpRow(rowOpt: Option[Row[SoQLValue]], pfx: String) {
    rowOpt match {
      case Some(row) =>
        val hd :: tl = jsonify(row).toString.split('\n').toList
        println(pfx + hd)
        for(line <- tl) {
          println(" " * pfx.length + line)
        }
      case None =>
        println("[none]")
    }
  }

  for {
    ds <- DataSourceFromConfig(cfg)
    conn <- managed(ds.dataSource.getConnection())
  } {
    conn.setAutoCommit(false)
    conn.setReadOnly(true)
    val delogger: Delogger[SoQLValue] = new SqlDelogger(conn, table, () => SoQLRowLogCodec)
    for(i <- from to to) {
      println("Version " + i + ":")
      using(delogger.delog(i)) { it =>
        it.foreach {
          case rdu: RowDataUpdated[SoQLValue] =>
            println("  - RowDataUpdated")
            rdu.operations.foreach {
              case Insert(sid, row) =>
                println("    * Insert")
                print("      + sid: ")
                println(sid.underlying)
                dumpRow(Some(row), "      + data: ")
              case Update(sid, oldRow, newRow) =>
                println("    * Update")
                print("      + sid: ")
                println(sid.underlying)
                dumpRow(oldRow, "      + old data: ")
                dumpRow(Some(newRow), "      + new data: ")
              case Delete(sid, oldRow) =>
                println("    * Delete")
                print("      + sid: ")
                println(sid.underlying)
                dumpRow(oldRow, "      + old data: ")
            }
          case other =>
            print("  - ")
            println(other)
        }
      }
    }
  }
}
