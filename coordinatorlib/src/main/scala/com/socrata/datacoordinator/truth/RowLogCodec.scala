package com.socrata.datacoordinator
package truth

import java.io.{EOFException, IOException}

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import gnu.trove.map.hash.{TLongIntHashMap, TIntLongHashMap}
import gnu.trove.impl.Constants

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

trait IdCachingRowLogCodec[CV] extends RowLogCodec[CV] {
  private val colNameReadCache = new TIntLongHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, -1)
  private val colNameWriteCache = new TLongIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, -1)

  private def writeKey(target: CodedOutputStream, key: ColumnId) {
    val cached = colNameWriteCache.get(key.underlying)
    if(cached == -1) {
      val id = colNameWriteCache.size()
      colNameWriteCache.put(key.underlying, id)
      target.writeInt32NoTag(id)
      target.writeInt64NoTag(key.underlying)
    } else {
      target.writeInt32NoTag(cached)
    }
  }

  private def readKey(source: CodedInputStream): ColumnId = {
    val id = source.readInt32()
    val cached = colNameReadCache.get(id)
    if(cached == -1) {
      val key = source.readInt64()
      colNameReadCache.put(id, key)
      new ColumnId(key)
    } else {
      new ColumnId(cached)
    }
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

object RowLogCodec {
  sealed abstract class Operation[+CV]
  case class Insert[CV](systemID: Long, row: Row[CV]) extends Operation[CV]
  case class Update[CV](systemID: Long, row: Row[CV]) extends Operation[CV]
  case class Delete(systemID: Long) extends Operation[Nothing]
}
