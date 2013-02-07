package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.packets.{Packet, PacketOutputStream}
import java.nio.ByteBuffer
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.loader.Delogger
import java.io._
import com.socrata.datacoordinator.common.util.ByteBufferInputStream
import com.socrata.datacoordinator.truth.RowLogCodec
import com.rojoma.json.util.JsonUtil
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import scala.Some

class Protocol[CV](rowLogCodecFactory: () => RowLogCodec[CV]) {
  import Protocol._

  // primary -> backup
  object NothingYet extends SimplePacket("nothing yet")
  object DataDone extends SimplePacket("data done")
  object WillResync extends SimplePacket("will resync")

  object DatasetUpdated {
    private val label = "dataset updated"

    def apply(id: DatasetId, version: Long) = {
      val pos = new PacketOutputStream
      labelPacket(pos, label)
      val dos = new DataOutputStream(pos)
      dos.writeLong(id.underlying)
      dos.writeLong(version)
      dos.flush()
      pos.packet()
    }

    def unapply(packet: Packet): Option[(DatasetId, Long)] =
      for(data <- packetLabelled(packet, label)) yield {
        if(data.remaining != 16) throw new PacketDecodeException("DatasetUpdate packet does not contain 16 bytes of payload")
        val id = new DatasetId(data.getLong())
        val version = data.getLong()
        (id, version)
      }
  }

  object LogData {
    def apply(event: Delogger.LogEvent[CV]) = {
      val pos = new PacketOutputStream
      pos.write(("log data|" + event.productPrefix).getBytes)
      pos.write(0)
      event match {
        case Delogger.RowDataUpdated(bytes) =>
          pos.write(bytes)
        case Delogger.RowIdCounterUpdated(rid) =>
          pos.write(rid.underlying.toString.getBytes("UTF-8"))
        case Delogger.WorkingCopyCreated(ci) =>
          pos.write(JsonUtil.renderJson(ci).getBytes("UTF-8"))
        case Delogger.WorkingCopyPublished | Delogger.WorkingCopyDropped | Delogger.DataCopied =>
          /* pass */
        case Delogger.ColumnCreated(col) =>
          pos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
        case Delogger.RowIdentifierSet(col) =>
          pos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
        case Delogger.SystemRowIdentifierChanged(col) =>
          pos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      }
      pos.packet()
    }

    def unapply(packet: Packet): Option[Delogger.LogEvent[CV]] =
      for(data <- packetLabelled(packet, "log data")) yield {
        val stream = new ByteBufferInputStream(data)
        val ev = eventType(stream)
        ev match {
          case "RowDataUpdated" =>
            val bytes = new Array[Byte](stream.remaining)
            stream.read(bytes)
            Delogger.RowDataUpdated(bytes)(rowLogCodecFactory())
          case "RowIdCounterUpdated" =>
            val rid = try {
              new BufferedReader(new InputStreamReader(stream, "UTF-8")).readLine().toLong
            } catch {
              case _: NullPointerException | _: NumberFormatException =>
                throw new PacketDecodeException("Unable to decode a row identifier")
            }
            Delogger.RowIdCounterUpdated(new RowId(rid))
          case "WorkingCopyCreated" =>
            val ci = JsonUtil.readJson[CopyInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
              throw new PacketDecodeException("Unable to decode a copyinfo")
            }
            Delogger.WorkingCopyCreated(ci)
          case "WorkingCopyPublished" =>
            Delogger.WorkingCopyPublished
          case "WorkingCopyDropped" =>
            Delogger.WorkingCopyDropped
          case "DataCopied" =>
            Delogger.DataCopied
          case "ColumnCreated" =>
            val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
              throw new PacketDecodeException("Unable to decode a columnInfo")
            }
            Delogger.ColumnCreated(ci)
          case "RowIdentifierSet" =>
            val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
              throw new PacketDecodeException("Unable to decode a columnInfo")
            }
            Delogger.RowIdentifierSet(ci)
          case "SystemRowIdentifierChanged" =>
            val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
              throw new PacketDecodeException("Unable to decode a columnInfo")
            }
            Delogger.SystemRowIdentifierChanged(ci)
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

  // backup -> primary
  object OkStillWaiting extends SimplePacket("ok still waiting")
  object AlreadyHaveThat extends SimplePacket("already have that")
  object ResyncRequired extends SimplePacket("resync required")
  object WillingToAccept extends SimplePacket("willing to accept")
  object AcknowledgeReceipt extends SimplePacket("acknowledged")
}

object Protocol {
  class SimplePacket(s: String) {
    val data = locally {
      val pos = new PacketOutputStream
      pos.write(s.getBytes)
      pos.packet()
    }

    def apply() = data
    def unapply(p: Packet): Boolean = p == data
  }

  def labelSep = '|'

  def packetLabelled(packet: Packet, s: String): Option[ByteBuffer] = {
    if(packet.dataSize >= s.length + 1) {
      val data = packet.data
      var i = 0
      while(i != s.length && data.get() == s.charAt(i).toByte) { i += 1 }
      if(i == s.length && data.get() == labelSep) Some(data)
      else None
    } else {
      None
    }
  }

  def labelPacket(pos: OutputStream, s: String) {
    var i = 0
    while(i != s.length) {
      pos.write(s.charAt(i).toByte)
      i += 1
    }
    pos.write(labelSep.toByte)
  }
}

class PacketDecodeException(msg: String) extends Exception(msg)
