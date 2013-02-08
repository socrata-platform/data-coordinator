package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.packets.{Packet, PacketOutputStream}
import java.nio.ByteBuffer
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.loader.Delogger
import java.io._
import com.socrata.datacoordinator.common.util.ByteBufferInputStream
import com.rojoma.json.util.JsonUtil
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import scala.Some

class Protocol[LogData](logDataCodec: Codec[LogData]) {
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
    def apply(event: LogData) = {
      val pos = new PacketOutputStream
      val dos = new DataOutputStream(pos)
      dos.write("log data|".getBytes)
      logDataCodec.encode(dos, event)
      dos.flush()
      pos.packet()
    }

    def unapply(packet: Packet): Option[LogData] = {
      for(data <- packetLabelled(packet, "log data")) yield {
        val stream = new DataInputStream(new ByteBufferInputStream(data))
        logDataCodec.decode(stream)
      }
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
      pos.write(s.getBytes("UTF-8"))
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
