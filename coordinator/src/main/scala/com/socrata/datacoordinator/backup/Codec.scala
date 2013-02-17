package com.socrata.datacoordinator.backup

import java.io.{InputStreamReader, InputStream, DataInputStream, DataOutputStream}

import com.rojoma.json.util.JsonUtil

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.loader.Delogger.LogEvent
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.RowLogCodec

trait Codec[T] {
  def encode(target: DataOutputStream, data: T)
  def decode(input: DataInputStream): T
}

class LogDataCodec[CV](rowLogCodecFactory: () => RowLogCodec[CV]) extends Codec[Delogger.LogEvent[CV]] {
  def encode(dos: DataOutputStream, event: LogEvent[CV]) {
    dos.write(event.productPrefix.getBytes)
    dos.write(0)
    event match {
      case Delogger.RowDataUpdated(bytes) =>
        dos.writeInt(bytes.length)
        dos.write(bytes)
      case Delogger.RowIdCounterUpdated(rid) =>
        dos.writeLong(rid.underlying)
      case Delogger.WorkingCopyCreated(di, ci) =>
        dos.write(JsonUtil.renderJson(di).getBytes("UTF-8"))
        dos.write('\n')
        dos.write(JsonUtil.renderJson(ci).getBytes("UTF-8"))
      case Delogger.WorkingCopyPublished | Delogger.WorkingCopyDropped | Delogger.DataCopied | Delogger.Truncated =>
        /* pass */
      case Delogger.ColumnCreated(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.RowIdentifierSet(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.RowIdentifierCleared(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.ColumnRemoved(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.SystemRowIdentifierChanged(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.EndTransaction =>
        sys.error("Shouldn't have seen EndTransaction")
    }
  }

  def decode(stream: DataInputStream): Delogger.LogEvent[CV] = {
    val ev = Delogger.LogEvent.fromProductName(eventType(stream))
    ev match {
      case Delogger.RowDataUpdated =>
        val count = stream.readInt()
        val bytes = new Array[Byte](count)
        stream.readFully(bytes)
        Delogger.RowDataUpdated(bytes)(rowLogCodecFactory())
      case Delogger.RowIdCounterUpdated =>
        val rid = new RowId(stream.readLong())
          Delogger.RowIdCounterUpdated(rid)
      case Delogger.WorkingCopyCreated =>
        val r = new InputStreamReader(stream, "UTF-8")
        val di = JsonUtil.readJson[UnanchoredDatasetInfo](r).getOrElse {
          throw new PacketDecodeException("Unable to decode a datasetinfo")
        }
        val ci = JsonUtil.readJson[UnanchoredCopyInfo](r).getOrElse {
          throw new PacketDecodeException("Unable to decode a copyinfo")
        }
        Delogger.WorkingCopyCreated(di, ci)
      case Delogger.WorkingCopyPublished =>
        Delogger.WorkingCopyPublished
      case Delogger.WorkingCopyDropped =>
        Delogger.WorkingCopyDropped
      case Delogger.DataCopied =>
        Delogger.DataCopied
      case Delogger.Truncated =>
        Delogger.Truncated
      case Delogger.ColumnCreated =>
        val ci = JsonUtil.readJson[UnanchoredColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.ColumnCreated(ci)
      case Delogger.RowIdentifierSet =>
        val ci = JsonUtil.readJson[UnanchoredColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.RowIdentifierSet(ci)
      case Delogger.RowIdentifierCleared =>
        val ci = JsonUtil.readJson[UnanchoredColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.RowIdentifierCleared(ci)
      case Delogger.ColumnRemoved =>
        val ci = JsonUtil.readJson[UnanchoredColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.ColumnRemoved(ci)
      case Delogger.SystemRowIdentifierChanged =>
        val ci = JsonUtil.readJson[UnanchoredColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.SystemRowIdentifierChanged(ci)
      case Delogger.EndTransaction =>
        Delogger.EndTransaction
    }
  }

  def eventType(in: InputStream) = {
    val sb = new java.lang.StringBuilder
    def loop() {
      in.read() match {
        case -1 => throw new PacketDecodeException("LogData packet truncated before the event type")
        case 0 => // done
        case c => sb.append(c.toChar); loop()
      }
    }
    loop()
    sb.toString
  }
}
