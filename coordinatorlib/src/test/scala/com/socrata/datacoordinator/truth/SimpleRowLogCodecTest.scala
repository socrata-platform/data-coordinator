package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.truth.loader.Insert
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import java.io.ByteArrayOutputStream
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap

class SimpleRowLogCodecTest extends FunSuite with MustMatchers with ScalaCheckPropertyChecks {
  def serialize[T](codec: SimpleRowLogCodec[T], row: Row[T]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val cos = CodedOutputStream.newInstance(baos)
    codec.insert(cos, new RowId(0), row)
    cos.flush()
    baos.toByteArray
  }

  def deserialize[T](codec: SimpleRowLogCodec[T], bs: Array[Byte]): Row[T] = {
    val cis = CodedInputStream.newInstance(bs)
    codec.extract(cis).getOrElse {
      fail("No data in source")
    } match {
      case Insert(rid, r) => r
      case _ => fail("No insert in source")
    }
  }

  def newCodec = new SimpleRowLogCodec[Int] {
    def rowDataVersion: Short = 0

    protected def writeValue(target: CodedOutputStream, cv: Int) {
      target.writeSInt32NoTag(cv)
    }

    protected def readValue(source: CodedInputStream): Int =
      source.readSInt32()
  }

  def toRow[T](m: Map[Long, T]): Row[T] = {
    val r = new MutableColumnIdMap[T]
    for((k, v) <- m) r(new ColumnId(k)) = v
    r.freeze()
  }

  test("Serializing and deserializing must produce the same row") {
    forAll { (rawRow: Map[Long, Int]) =>
      val codec = newCodec
      val row = toRow(rawRow)
      val serialized = serialize(codec, row)
      val deserialized = deserialize(codec, serialized)
      deserialized must equal (row)
    }
  }

  test("Serializing, deserializing, and reserializing must produce the same bytes for both serializations") {
    forAll { (rawRow: Map[Long, Int]) =>
      val codec = newCodec
      val row = toRow(rawRow)
      val serializedOnce = serialize(codec, row)
      val deserialized = deserialize(codec, serializedOnce)
      val serializedAgain = serialize(codec, deserialized)
      serializedOnce must equal (serializedAgain)
    }
  }
}
