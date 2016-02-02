package com.socrata.datacoordinator.service

import java.io.{OutputStream, Reader}
import java.nio.charset.StandardCharsets._

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.ast.{JString, JObject}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.util.IndexedTempFile

class ReaderExceededBound(val bytesRead: Long) extends Exception
class BoundedReader(underlying: Reader, bound: Long) extends Reader {
  private var count = 0L
  private def inc(n: Int) {
    count += n
    if(count > bound) throw new ReaderExceededBound(count)
  }

  override def read() =
    underlying.read() match {
      case -1 => -1
      case n => inc(1); n
    }

  def read(cbuf: Array[Char], off: Int, len: Int): Int =
    underlying.read(cbuf, off, len) match {
      case -1 => -1
      case n => inc(n); n
    }

  def close(): Unit = {
    underlying.close()
  }

  def resetCount(): Unit = {
    count = 0
  }
}


object ServiceUtil {
  val JsonContentType = "application/json; charset=utf-8"
  val TextContentType = "text/plain; charset=utf-8"


  def jsonifySchema(schemaObj: Schema): JValue = {
    val Schema(hash, schema, pk, locale) = schemaObj
    val jsonSchema = JObject(schema.iterator.map { case (k,v) => k.underlying -> JString(v.name) }.toMap)
    JObject(Map(
      "hash" -> JString(hash),
      "schema" -> jsonSchema,
      "pk" -> JsonEncode.toJValue(pk),
      "locale" -> JString(locale)
    ))
  }

  def writeResult(o: OutputStream, r: MutationScriptCommandResult, tmp: IndexedTempFile): Unit = {
    r match {
      case MutationScriptCommandResult.ColumnCreated(id, typname) =>
        o.write(CompactJsonWriter.toString(JObject(Map("id" -> JsonEncode.toJValue(id),
          "type" -> JString(typname.name)))).getBytes(UTF_8))
      case MutationScriptCommandResult.Uninteresting =>
        o.write('{')
        o.write('}')
      case MutationScriptCommandResult.RowData(jobs) =>
        o.write('[')
        jobs.foreach(new Function1[Long, Unit] {
          var didOne = false
          def apply(job: Long) {
            if(didOne) {
              o.write(',')
            } else {
              didOne = true
            }
            tmp.readRecord(job).get.writeTo(o)
          }
        })
        o.write(']')
    }
  }






}
