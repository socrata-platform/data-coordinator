package com.socrata.datacoordinator
package truth

import java.io.ByteArrayOutputStream

import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.util.RowUtils
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap
import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

class RowLogCodecTest extends FunSuite with MustMatchers with PropertyChecks {
  val InsertId = 0
  val UpdateNoOldRowId = 1
  val DeleteNoOldRowDataId = 2
  val UpdateId = 3
  val DeleteId = 4

  val codec = new TestCodec()

  class TestCodec(r: Short = 0, s: Short = 0) extends SimpleRowLogCodec[Int] {
    override def rowDataVersion: Short = r
    override def structureVersion: Short = s

    override protected def writeValue(target: CodedOutputStream, cv: Int): Unit = target.writeSInt32NoTag(cv)
    override protected def readValue(source: CodedInputStream): Int = source.readSInt32()

    // encode is a protected method.
    def doEncode(target: CodedOutputStream, row: Row[Int]) = encode(target, row)
  }

  def withOutput[T](body: CodedOutputStream => Unit): Seq[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val codedOutputStream = CodedOutputStream.newInstance(byteArrayOutputStream)
    body(codedOutputStream)

    codedOutputStream.flush()
    byteArrayOutputStream.toByteArray: Seq[Byte]
  }

  def toRow[T](m: Map[Long, T]): Row[T] = {
    val r = new MutableColumnIdMap[T]
    for((k, v) <- m) r(new ColumnId(k)) = v
    r.freeze()
  }

  /** Splits a Short into bytes (Little Endian) */
  def splitBytes(s: Short): Seq[Byte] = Seq((s << 8 >> 8).toByte, (s >> 8).toByte)

  def int64NoTag(l: Long): Seq[Byte] = withOutput {
    _.writeInt64NoTag(l)
  }

  test("Write version tag to stream") {
    forAll { (rowDataVersion: Short, structureVersion: Short) =>
      val bytes = withOutput(new TestCodec(rowDataVersion, structureVersion).writeVersion)

      bytes.length must equal(4)
      bytes.containsSlice(splitBytes(rowDataVersion)) must be (true)
      bytes.containsSlice(splitBytes(structureVersion)) must be (true)
    }
  }

  // insert() is already tested by SimpleRowLogCodecTest, perhaps duplicate some
  // logic here, too?

  test("Update new row") {
    forAll { (rawSystemId: Long, rawNewRow: Map[Long, Int]) =>
      val systemId = new RowId(rawSystemId)
      val newRow = toRow(rawNewRow)

      val bytes = withOutput {
        codec.update(_, systemId, None, newRow)
      }

      bytes.length must be >= 3
      bytes(0).toInt must equal (UpdateNoOldRowId)

      val serializedSysId = int64NoTag(systemId.underlying)
      bytes.containsSlice(serializedSysId) must be (true)
      bytes.containsSlice(withOutput(codec.doEncode(_, newRow))) must be (true)
    }
  }

  test("Update existing row") {
    forAll {
      (rawSystemId: Long,
        rawOldRow: Map[Long, Int],
        rawNewRow: Map[Long, Int]) =>
      val systemId = new RowId(rawSystemId)
      val oldRow = toRow(rawOldRow)
      val newRow = toRow(rawNewRow)

      val existingBytes = withOutput {
        codec.update(_, systemId, None, oldRow)
      }

      val rawBytes = withOutput { target =>
        codec.update(target, systemId, None, oldRow)
        codec.update(target, systemId, Some(oldRow), newRow)
      }

      rawBytes.take(existingBytes.length) must equal(existingBytes)

      val bytes = rawBytes.drop(existingBytes.length)
      bytes.length must be >= 3

      bytes(0) must equal(UpdateId)

      val serializedSysId = int64NoTag(systemId.underlying)
      bytes.containsSlice(serializedSysId) must be (true)

      val oldRowBytes = withOutput { codec.doEncode(_, oldRow) }
      val deltaRowBytes = withOutput {
        codec.doEncode(_, RowUtils.delta(oldRow, newRow))
      }

      bytes.containsSlice(oldRowBytes) must be (true)
      bytes.containsSlice(deltaRowBytes) must be (true)
    }
  }

  test("Delete existing row") {
  }

  test("Delete missing row") {

  }

  test("Move cursor past version tag") {
  }

  // extract() is already tested by SimpleRowLogCodecTest as well.
}
