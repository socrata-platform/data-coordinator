package com.socrata.datacoordinator
package truth

import java.io.{ByteArrayOutputStream, EOFException, InputStream}

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.truth.loader.{Update, Insert, Delete}
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

  def withOutput(body: CodedOutputStream => Unit): Seq[Byte] = {
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

      bytes.length must equal (4)
      bytes must equal (splitBytes(rowDataVersion) ++ splitBytes(structureVersion))
    }
  }

  test("Write default structureVersion to stream") {
    forAll { dataVersion: Short =>
      val codec = new RowLogCodec[Int] {
        override def rowDataVersion: Short = dataVersion

        override protected def encode(target: CodedOutputStream, row: Row[Int]): Unit = {}
        override protected def decode(source: CodedInputStream): Row[Int] = null
      }

      val bytes = withOutput(codec.writeVersion)
      bytes.length must equal (4)
      bytes must equal (splitBytes(dataVersion) ++ splitBytes(codec.structureVersion))
    }
  }

  test("Insert a new row") {
    forAll { (rawSystemId: Long, rawNewRow: Map[Long, Int]) =>
      val systemId = new RowId(rawSystemId)
      val newRow = toRow(rawNewRow)

      val bytes = withOutput {
        codec.insert(_, systemId, newRow)
      }

      bytes.length must be >= 3
      bytes(0).toInt must equal (InsertId)

      val serializedSysId = int64NoTag(systemId.underlying)
      bytes.containsSlice(serializedSysId) must be (true)
      bytes.containsSlice(withOutput(codec.doEncode(_, newRow))) must be (true)
    }
  }

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
        codec.insert(_, systemId, oldRow)
      }

      val rawBytes = withOutput { target =>
        codec.insert(target, systemId, oldRow)
        codec.update(target, systemId, Some(oldRow), newRow)
      }

      rawBytes.take(existingBytes.length) must equal (existingBytes)

      val bytes = rawBytes.drop(existingBytes.length)
      bytes.length must be >= 3

      bytes(0) must equal (UpdateId)

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
    forAll { (rawSystemId: Long, rawRow: Map[Long, Int]) =>
      val systemId = new RowId(rawSystemId)
      val row = toRow(rawRow)

      val bytes = withOutput {
        codec.delete(_, systemId, Some(row))
      }

      bytes(0) must equal (DeleteId)
      bytes.containsSlice(int64NoTag(systemId.underlying)) must be (true)
      bytes.containsSlice(withOutput(codec.doEncode(_, row))) must be (true)
    }
  }

  test("Delete missing row") {
    forAll { rawSystemId: Long =>
      val systemId = new RowId(rawSystemId)

      val bytes = withOutput {
        codec.delete(_, systemId, None)
      }

      bytes(0) must equal (DeleteNoOldRowDataId)
      bytes.containsSlice(int64NoTag(systemId.underlying)) must be (true)
    }
  }

  test("Move cursor past version tag") {
    forAll { (rowDataVersion: Short, structureVersion: Short) =>
      val codec = new TestCodec(rowDataVersion, structureVersion)
      val versionBytes = withOutput(codec.writeVersion)
      val inputStream = CodedInputStream.newInstance(versionBytes.toArray)

      codec.skipVersion(inputStream)

      inputStream.isAtEnd must be (true)
    }
  }

  test("With no data extract returns 'None'") {
    val extracted = codec.extract(CodedInputStream.newInstance(new Array[Byte](0)))

    extracted must equal (None)
  }

  test("Extract wraps InvalidProtocolBufferException and EOFException") {
    val invalidProtoBufEx = new InvalidProtocolBufferException("IPB Exception")
    val eofEx = new EOFException("EOF Exception")

    class ExStream(ex: Exception) extends InputStream {
      override def read(): Int = throw ex
    }

    def exStream(ex: Exception) = CodedInputStream.newInstance(new InputStream {
      override def read(): Int = throw ex
    })

    val thrownInvalidProtoBuf = evaluating {
      codec.extract(exStream(invalidProtoBufEx))
    } must produce [RowLogTruncatedException]

    thrownInvalidProtoBuf.getCause must equal (invalidProtoBufEx)

    val thrownEOF = evaluating {
      codec.extract(exStream(eofEx))
    } must produce [RowLogTruncatedException]

    thrownEOF.getCause must equal (eofEx)
  }

  test("Extract throws an exception on unknown operation") {
    val validOps = Seq(InsertId, UpdateNoOldRowId, DeleteNoOldRowDataId, UpdateId, DeleteId)
    forAll { operation: Byte =>
      whenever (!validOps.contains(operation)) {
        val opBytes = withOutput(_.writeRawByte(operation)).toArray

        val thrown = evaluating {
          codec.extract(CodedInputStream.newInstance(opBytes))
        } must produce [UnknownRowLogOperationException]

        thrown.operationCode must equal (operation)
      }
    }
  }

  test("Operations are extracted") {
    def stream(body: CodedOutputStream => Unit) = {
      CodedInputStream.newInstance(withOutput(body).toArray)
    }

    forAll { (
      rawSystemId: Long,
      rawOldRow: Map[Long, Int],
      rawNewRow: Map[Long, Int]) =>

      val systemId = new RowId(rawSystemId)
      val oldRow = toRow(rawOldRow)
      val newRow = toRow(rawNewRow)

      val insertStream = stream { codec.insert(_, systemId, newRow) }
      codec.extract(insertStream) must equal (Some(Insert(systemId, newRow)))

      val newUpdateStream = stream { codec.update(_, systemId, None, newRow) }
      codec.extract(newUpdateStream) must equal (
        Some(Update(systemId, None, newRow)))

      val oldUpdateStream = stream {
        codec.update(_, systemId, Some(oldRow), newRow)
      }
      codec.extract(oldUpdateStream) must equal (
        Some(Update(systemId, Some(oldRow), oldRow ++ newRow)))

      val newDeleteStream = stream { codec.delete(_, systemId, None) }
      codec.extract(newDeleteStream) must equal (Some(Delete(systemId, None)))

      val oldDeleteStream = stream { codec.delete(_, systemId, Some(oldRow)) }
      codec.extract(oldDeleteStream) must equal (
        Some(Delete(systemId, Some(oldRow))))
    }
  }
}
