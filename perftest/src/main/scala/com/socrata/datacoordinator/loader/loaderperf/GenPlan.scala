package com.socrata.datacoordinator.loader
package loaderperf

import java.util.zip.GZIPOutputStream
import java.io.{Writer, BufferedWriter, OutputStreamWriter, FileOutputStream}

import com.rojoma.json.ast._
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JString

import com.rojoma.simplearm.util._
import com.rojoma.json.io.{JsonWriter, CompactJsonWriter}
import com.rojoma.json.codec.JsonCodec

object GenPlan {
  val initialSize = 1000000
  val insertNew = 90000
  val updateOld = 200000
  val deleteOld = 0
  val nullFrac = 0.2 // odds that a value will be null
  val updateFrac = 0.5 // odds that a cell will be chosen for updating

  val seed = 54892735234L
  val rng = new scala.util.Random(seed)

  val Text = 0
  val Numeric = 1

  def randomText() = {
    val sb = new StringBuilder
    val len = rng.nextInt(100) + 1
    for(_ <- 0 until len) sb += (rng.nextInt(127 - 32) + 32).toChar
    sb.toString
  }

  def randomNumber() = BigDecimal(rng.nextLong()) / BigDecimal(100000)

  def randomValue(typ: Int): JAtom =
    if(rng.nextFloat() <= nullFrac) JNull
    else typ match {
      case Text => JString(randomText())
      case Numeric => JNumber(randomNumber())
    }

  def randomRow(schema: Map[String, Int]): JObject = {
    JObject(schema.foldLeft(Map.empty[String, JValue]) { (row, colTyp) =>
      val (col, typ) = colTyp
      row + (col -> randomValue(typ))
    })
  }

  def forUpdate(row: JObject): JObject = {
    JObject(row.fields.foldLeft(Map.empty[String, JValue]) { (row, colVal) =>
      if(rng.nextFloat() <= updateFrac) row + colVal
      else row
    })
  }

  def typeName(typ: Int) = typ match {
    case Text => "TEXT"
    case Numeric => "NUMERIC"
  }

  def genSchema(): Map[String, Int] = {
    val columnCount = 5 + rng.nextInt(50)
    (1 to columnCount).foldLeft(Map.empty[String, Int]) { (schema, i) =>
      val name = "col" + i
      val typ = rng.nextInt(2)
      schema + (name -> typ)
    }
  }

  object nextId {
    var x = 0L
    def apply() = { x += 1; x }
  }

  def prepopulate(writer: Writer, output: JsonWriter, schema: Map[String, Int]): (Choosable[Long], Map[Long, BigDecimal]) = {
    val c = new Choosable[Long](rng)
    var i = 0
    var map = Map.empty[Long, BigDecimal]
    while(i != initialSize) {
      val rowId = randomNumber()
      val id = nextId()
      output.write(JsonCodec.toJValue(Insert(id, JObject(schema.mapValues(randomValue) + ("uid" -> JNumber(rowId))))))
      writer.write('\n')
      c += id
      map += id -> rowId
      i += 1
    }
    (c, map)
  }

  def main(args: Array[String]) {
    using(new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(args(0))), "UTF-8"))) { writer =>
      val jWriter = new CompactJsonWriter(writer)
      val schema = genSchema()
      jWriter.write(JsonCodec.toJValue(schema.mapValues(typeName) + ("uid" -> "NUMERIC")))

      println("Prepopulating...")
      var (currentIds, currentUids) = prepopulate(writer, jWriter, schema)

      writer.write("\n")
      jWriter.write(JString("------"))
      writer.write("\n")

      println("Building plan...")

      var remainingInserts = insertNew
      var remainingUpdates = updateOld
      var remainingDeletes = deleteOld

      def remainingOps = remainingInserts + remainingUpdates + remainingDeletes

      while(remainingOps > 0) {
        val n = rng.nextInt(remainingOps)
        if(n < remainingInserts || currentIds.isEmpty) {
          val id = nextId()
          val row = randomRow(schema)
          val randomUid = randomNumber()
          jWriter.write(JsonCodec.toJValue[Operation](Insert(id, JObject(row.fields + ("uid" -> JNumber(randomUid))))))
          currentUids += id -> randomUid
          writer.write("\n")
          currentIds += id
          remainingInserts -= 1
        } else if(n - remainingInserts < remainingUpdates) {
          val id = currentIds.pick()
          val row = JObject(forUpdate(randomRow(schema)).fields + ("uid" -> JNumber(currentUids(id))))
          if(!row.isEmpty) {
            jWriter.write(JsonCodec.toJValue[Operation](Update(id, row)))
            writer.write("\n")
            remainingUpdates -= 1
          }
        } else {
          val id = currentIds.extract()
          jWriter.write(JsonCodec.toJValue[Operation](Delete(id, JNumber(currentUids(id)))))
          currentUids -= id
          writer.write("\n")
          remainingDeletes -= 1
        }
      }

      jWriter.write(JString("------"))
      writer.write("\n")
    }
  }
}
