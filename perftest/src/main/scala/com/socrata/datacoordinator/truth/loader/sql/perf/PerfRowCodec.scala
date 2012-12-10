package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.{UnknownDataTypeException, RowLogCodec}
import gnu.trove.map.hash.{TObjectIntHashMap, TIntObjectHashMap}
import gnu.trove.impl.Constants

import com.google.protobuf.{CodedInputStream, CodedOutputStream, InvalidProtocolBufferException}

class PerfRowCodec extends RowLogCodec[PerfValue] {
  def rowDataVersion = 0

  val UTF8 = scala.io.Codec.UTF8

  private val colNameReadCache = new TIntObjectHashMap[String]
  private val colNameWriteCache = new TObjectIntHashMap[String](Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1)

  private def writeKey(target: CodedOutputStream, key: String) {
    val cached = colNameWriteCache.get(key)
    if(cached == -1) {
      val id = colNameWriteCache.size()
      colNameWriteCache.put(key, id)
      target.writeInt32NoTag(id)
      target.writeStringNoTag(key)
    } else {
      target.writeInt32NoTag(cached)
    }
  }

  private def readKey(source: CodedInputStream): String = {
    val id = source.readInt32()
    val cached = colNameReadCache.get(id)
    if(cached == null) {
      val key = source.readString()
      colNameReadCache.put(id, key)
      key
    } else {
      cached
    }
  }

  protected def encode(target: CodedOutputStream, row: Row[PerfValue]) {
    target.writeInt32NoTag(row.size)
    for((k,v) <- row) {
      writeKey(target, k)
      v match {
        case PVId(i) =>
          target.writeRawByte(0)
          target.writeInt64NoTag(i)
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
    val result = Map.newBuilder[String, PerfValue]
    for(i <- 0 until count) {
      val k = readKey(source)
      val v = source.readRawByte() match {
        case 0 =>
          PVId(source.readInt64())
        case 1 =>
          PVText(source.readString())
        case 2 =>
          PVNumber(BigDecimal(source.readString()))
        case 3 =>
          PVNull
        case other =>
          throw new UnknownDataTypeException(other)
      }
      result += k -> v
    }
    result.result()
  }
}
