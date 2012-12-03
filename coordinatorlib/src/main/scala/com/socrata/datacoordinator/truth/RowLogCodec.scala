package com.socrata.datacoordinator
package truth

import java.io.{EOFException, IOException, DataInputStream, DataOutputStream}

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
  protected def encode(target: DataOutputStream, row: Row[CV])
  protected def decode(source: DataInputStream): Row[CV]

  def writeVersion(target: DataOutputStream) {
    target.writeShort(structureVersion)
    target.writeShort(rowDataVersion)
  }

  def insert(target: DataOutputStream, systemID: Long, row: Row[CV]) {
    target.writeByte(0)
    target.writeLong(systemID)
    encode(target, row)
  }

  def update(target: DataOutputStream, systemID: Long, row: Row[CV]) {
    target.writeByte(1)
    target.writeLong(systemID)
    encode(target, row)
  }

  def delete(target: DataOutputStream, systemID: Long) {
    target.writeByte(2)
    target.writeLong(systemID)
  }

  def skipVersion(source: DataInputStream) {
    source.readShort()
    source.readShort()
  }

  def extract(source: DataInputStream): Option[Operation[CV]] = {
    try {
      source.read() match {
        case -1 =>
          None
        case 0 =>
          val sid = source.readLong()
          val row = decode(source)
          Some(Insert(sid, row))
        case 1 =>
          val sid = source.readLong()
          val row = decode(source)
          Some(Update(sid, row))
        case 2 =>
          val sid = source.readLong()
          Some(Delete(sid))
        case other =>
          throw new UnknownRowLogOperationException(other)
      }
    } catch {
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
