package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.{UnknownDataTypeException, RowLogCodec}
import java.io.{DataOutputStream, DataInputStream}
import gnu.trove.map.hash.{TObjectShortHashMap, TShortObjectHashMap}
import gnu.trove.impl.Constants

class PerfRowCodec extends RowLogCodec[PerfValue] {
  def rowDataVersion = 0

  val UTF8 = scala.io.Codec.UTF8

  // as long as keys are >2 bytes long (and most will be!) and referenced more than once, this is a space win.
  // Even if they AREN'T referenced more than once, it's only two bytes of loss per key.
  private val colNameReadCache = new TShortObjectHashMap[String]
  private val colNameWriteCache = new TObjectShortHashMap[String](Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1)

  private def writeKey(target: DataOutputStream, key: String) {
    val cached = colNameWriteCache.get(key)
    if(cached == Short.MaxValue) {
      val id = colNameWriteCache.size().toShort
      colNameWriteCache.put(key, id)
      target.writeShort(id)
      target.writeUTF(key)
    } else {
      target.writeShort(cached)
    }
  }

  private def readKey(source: DataInputStream): String = {
    val id = source.readShort()
    val cached = colNameReadCache.get(id)
    if(cached == null) {
      val key = source.readUTF()
      colNameReadCache.put(id, key)
      key
    } else {
      cached
    }
  }

  protected def encode(target: DataOutputStream, row: Row[PerfValue]) {
    target.writeInt(row.size)
    for((k,v) <- row) {
      writeKey(target, k)
      v match {
        case PVId(i) =>
          target.writeByte(0)
          target.writeLong(i)
        case PVText(s) =>
          target.writeByte(1)
          val bytes = s.getBytes(UTF8)
          target.writeInt(bytes.length)
          target.write(bytes)
        case PVNumber(n) =>
          target.writeByte(2)
          target.writeUTF(n.toString)
        case PVNull =>
          target.writeByte(3)
      }
    }
  }

  protected def decode(source: DataInputStream) = {
    val count = source.readInt()
    val result = Map.newBuilder[String, PerfValue]
    for(i <- 0 until count) {
      val k = readKey(source)
      val v = source.readByte() match {
        case 0 =>
          PVId(source.readLong())
        case 1 =>
          val len = source.readInt()
          val bytes = new Array[Byte](len)
          source.readFully(bytes)
          PVText(new String(bytes, UTF8))
        case 2 =>
          PVNumber(BigDecimal(source.readUTF()))
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
