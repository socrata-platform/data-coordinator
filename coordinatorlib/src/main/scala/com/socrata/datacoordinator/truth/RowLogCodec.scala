package com.socrata.datacoordinator
package truth

import java.io.{EOFException, IOException}

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}

// Hm, may want to refactor this somewhat.  In particular, we'll
// probably want to plug in different decoders depending on
// the version read from the stream.
//
// A RowLogCodec is allowed to be stateful (e.g., to cache column names)
// and so should be re-created for every log row.
trait RowLogCodec[CV] {
  import RowLogCodec._

  def structureVersion: Short = 0

  def rowDataVersion: Short
  protected def encode(target: CodedOutputStream, row: Row[CV])
  protected def decode(source: CodedInputStream): Row[CV]

  def writeVersion(target: CodedOutputStream) {
    target.writeFixed32NoTag((structureVersion.toInt << 16) | (rowDataVersion & 0xffff))
  }

  def insert(target: CodedOutputStream, systemID: Long, row: Row[CV]) {
    target.writeRawByte(0)
    target.writeInt64NoTag(systemID)
    encode(target, row)
  }

  def update(target: CodedOutputStream, systemID: Long, row: Row[CV]) {
    target.writeRawByte(1)
    target.writeInt64NoTag(systemID)
    encode(target, row)
  }

  def delete(target: CodedOutputStream, systemID: Long) {
    target.writeRawByte(2)
    target.writeInt64NoTag(systemID)
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
            Insert(sid, row)
          case 1 =>
            val sid = source.readInt64()
            val row = decode(source)
            Update(sid, row)
          case 2 =>
            val sid = source.readInt64()
            Delete(sid)
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

abstract class CorruptRowLogException(msg: String, cause: Throwable = null) extends RuntimeException(msg)
class RowLogTruncatedException(cause: IOException) extends CorruptRowLogException("Row log truncated", cause)
class UnknownRowLogOperationException(val operationCode: Int) extends CorruptRowLogException("Unknown operation " + operationCode)
class UnknownDataTypeException(val typeCode: Int) extends CorruptRowLogException("Unknown data type " + typeCode)

object RowLogCodec {
  sealed abstract class Operation[+CV]
  case class Insert[CV](systemID: Long, row: Row[CV]) extends Operation[CV]
  case class Update[CV](systemID: Long, row: Row[CV]) extends Operation[CV]
  case class Delete(systemID: Long) extends Operation[Nothing]
}
