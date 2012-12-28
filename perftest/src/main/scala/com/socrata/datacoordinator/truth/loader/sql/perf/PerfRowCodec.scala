package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.{UnknownDataTypeException, RowLogCodec}
import gnu.trove.map.hash.{TLongIntHashMap, TIntLongHashMap, TObjectIntHashMap, TIntObjectHashMap}
import gnu.trove.impl.Constants

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}
import com.socrata.datacoordinator.id.{RowId, ColumnId}

class PerfRowCodec extends RowLogCodec[PerfValue] {
  def rowDataVersion = 0

  val UTF8 = scala.io.Codec.UTF8

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

  protected def encode(target: CodedOutputStream, row: Row[PerfValue]) {
    target.writeInt32NoTag(row.size)
    val it = row.iterator
    while(it.hasNext) {
      it.advance()
      val k = it.key
      val v = it.value
      writeKey(target, k)
      v match {
        case PVId(i) =>
          target.writeRawByte(0)
          target.writeInt64NoTag(i.underlying)
        case PVText(s) =>
          target.writeRawByte(1)
          target.writeStringNoTag(s)
        case PVNumber(n) =>
          target.writeRawByte(2)
          target.writeStringNoTag(n.toString)
        case PVNull =>
          target.writeRawByte(3)
      }
    }
  }

  protected def decode(source: CodedInputStream) = {
    val count = source.readInt32()
    val result = new MutableRow[PerfValue]
    for(i <- 0 until count) {
      val k = readKey(source)
      val v = source.readRawByte() match {
        case 0 =>
          PVId(new RowId(source.readInt64()))
        case 1 =>
          PVText(source.readString())
        case 2 =>
          PVNumber(BigDecimal(source.readString()))
        case 3 =>
          PVNull
        case other =>
          throw new UnknownDataTypeException(other)
      }
      result(k) = v
    }
    result.freeze()
  }
}
