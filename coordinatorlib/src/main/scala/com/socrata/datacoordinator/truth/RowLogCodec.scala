package com.socrata.datacoordinator
package truth

import java.io.{EOFException, IOException}

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}

import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.truth.loader.{Operation, Insert, Update, Delete}

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

  def writeVersion(target: CodedOutputStream) {
    target.writeFixed32NoTag((structureVersion.toInt << 16) | (rowDataVersion & 0xffff))
  }

  def insert(target: CodedOutputStream, systemID: RowId, row: Row[CV]) {
    target.writeRawByte(0)
    target.writeInt64NoTag(systemID.underlying)
    encode(target, row)
  }

  def update(target: CodedOutputStream, systemID: RowId, row: Row[CV]) {
    target.writeRawByte(1)
    target.writeInt64NoTag(systemID.underlying)
    encode(target, row)
  }

  def delete(target: CodedOutputStream, systemID: RowId) {
    target.writeRawByte(2)
    target.writeInt64NoTag(systemID.underlying)
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
          case 0 =>
            val sid = source.readInt64()
            val row = decode(source)
            Insert(new RowId(sid), row)
          case 1 =>
            val sid = source.readInt64()
            val row = decode(source)
            Update(new RowId(sid), row)
          case 2 =>
            val sid = source.readInt64()
            Delete(new RowId(sid))
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
    val it = row.iterator
    while(it.hasNext) {
      it.advance()
      val k = it.key
      val v = it.value
      writeKey(target, k)
      writeValue(target, v)
    }
  }

  protected def decode(source: CodedInputStream) = {
    val count = source.readInt32()
    val result = new MutableRow[CV]
    for(i <- 0 until count) {
      val k = readKey(source)
      val v = readValue(source)
      result(k) = v
    }
    result.freeze()
  }
}

abstract class CorruptRowLogException(msg: String, cause: Throwable = null) extends RuntimeException(msg)
class RowLogTruncatedException(cause: IOException) extends CorruptRowLogException("Row log truncated", cause)
class UnknownRowLogOperationException(val operationCode: Int) extends CorruptRowLogException("Unknown operation " + operationCode)
class UnknownDataTypeException(val typeCode: Int) extends CorruptRowLogException("Unknown data type " + typeCode)
