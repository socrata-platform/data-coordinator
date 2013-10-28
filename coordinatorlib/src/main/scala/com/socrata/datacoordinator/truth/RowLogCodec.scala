package com.socrata.datacoordinator
package truth

import java.io.{EOFException, IOException}

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}

import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.truth.loader.{Operation, Insert, Update, Delete}
import com.socrata.datacoordinator.util.RowUtils

// Hm, may want to refactor this somewhat.  In particular, we'll
// probably want to plug in different decoders depending on
// the version read from the stream.
//
// A RowLogCodec is allowed to be stateful (e.g., to cache column names)
// and so should be re-created for every log row.
trait RowLogCodec[CV] {
  def structureVersion: Short = 0

  def rowDataVersion: Short
  protected def encode(target: CodedOutputStream, row: Row[CV])
  protected def decode(source: CodedInputStream): Row[CV]

  private val InsertId = 0
  private val UpdateNoOldRowId = 1
  private val DeleteNoOldRowDataId = 2
  private val UpdateId = 3
  private val DeleteId = 4

  def writeVersion(target: CodedOutputStream) {
    target.writeFixed32NoTag((structureVersion.toInt << 16) | (rowDataVersion & 0xffff))
  }

  def insert(target: CodedOutputStream, systemID: RowId, row: Row[CV]) {
    target.writeRawByte(InsertId)
    target.writeInt64NoTag(systemID.underlying)
    encode(target, row)
  }

  def update(target: CodedOutputStream, systemID: RowId, oldRow: Option[Row[CV]], newRow: Row[CV]) {
    oldRow match {
      case Some(trueOldRow) =>
        target.writeRawByte(UpdateId)
        target.writeInt64NoTag(systemID.underlying)
        encode(target, trueOldRow)
        val trueNewRow = RowUtils.delta(trueOldRow, newRow)
        encode(target, trueNewRow)
      case None =>
        target.writeRawByte(UpdateNoOldRowId)
        target.writeInt64NoTag(systemID.underlying)
        encode(target, newRow)
    }
  }

  def delete(target: CodedOutputStream, systemID: RowId, oldRow: Option[Row[CV]]) {
    oldRow match {
      case Some(trueOldRow) =>
        target.writeRawByte(DeleteId)
        target.writeInt64NoTag(systemID.underlying)
        encode(target, trueOldRow)
      case None =>
        target.writeRawByte(DeleteNoOldRowDataId)
        target.writeInt64NoTag(systemID.underlying)
    }
  }

  def skipVersion(source: CodedInputStream) {
    source.readFixed32()
  }

  def extract(source: CodedInputStream): Option[Operation[CV]] = {
    try {
      if(source.isAtEnd) {
        None
      } else {
        val op = source.readRawByte() match {
          case InsertId =>
            val sid = source.readInt64()
            val row = decode(source)
            Insert(new RowId(sid), row)
          case UpdateNoOldRowId =>
            val sid = source.readInt64()
            val row = decode(source)
            Update(new RowId(sid), None, row)
          case DeleteNoOldRowDataId =>
            val sid = source.readInt64()
            Delete(new RowId(sid), None)
          case UpdateId =>
            val sid = source.readInt64()
            val oldRow = decode(source)
            val newRow = oldRow ++ decode(source)
            Update(new RowId(sid), Some(oldRow), newRow)
          case DeleteId =>
            val sid = source.readInt64()
            val oldRow = decode(source)
            Delete(new RowId(sid), Some(oldRow))
          case other =>
            throw new UnknownRowLogOperationException(other)
        }
        Some(op)
      }
    } catch {
      case e: InvalidProtocolBufferException =>
        throw new RowLogTruncatedException(e)
      case e: EOFException =>
        throw new RowLogTruncatedException(e)
    }
  }
}

trait SimpleRowLogCodec[CV] extends RowLogCodec[CV] {
  protected def writeKey(target: CodedOutputStream, key: ColumnId) {
    target.writeInt64NoTag(key.underlying)
  }

  protected def readKey(source: CodedInputStream): ColumnId = {
    new ColumnId(source.readInt64())
  }

  protected def writeValue(target: CodedOutputStream, cv: CV)
  protected def readValue(source: CodedInputStream): CV

  protected def encode(target: CodedOutputStream, row: Row[CV]) {
    target.writeInt32NoTag(row.size)

    // A property we want is that serialize(deserialize(serialize(x)) == serialize(x)
    // so we need to put the keys in the map in a defined order.
    val map = row.unsafeUnderlying
    val keys = map.keys()
    java.util.Arrays.sort(keys)
    var keyIdx = 0
    while(keyIdx < keys.length) {
      val key = keys(keyIdx)

      val k = new ColumnId(key)
      val v = map.get(key)
      writeKey(target, k)
      writeValue(target, v)

      keyIdx += 1
    }
  }

  protected def decode(source: CodedInputStream) = {
    val count = source.readInt32()
    val result = new MutableRow[CV]
    var i = 0
    while(i < count) {
      val k = readKey(source)
      val v = readValue(source)
      result(k) = v

      i += 1
    }
    result.freeze()
  }
}

abstract class CorruptRowLogException(msg: String, cause: Throwable = null) extends RuntimeException(msg)
class RowLogTruncatedException(cause: IOException) extends CorruptRowLogException("Row log truncated", cause)
class UnknownRowLogOperationException(val operationCode: Int) extends CorruptRowLogException("Unknown operation " + operationCode)
class UnknownDataTypeException(val typeCode: Int) extends CorruptRowLogException("Unknown data type " + typeCode)
