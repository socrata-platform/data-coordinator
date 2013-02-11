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
  import Packet.{SimplePacket, labelledPacketStream, packetLabelled}

  // primary -> backup
  object NothingYet extends SimplePacket("nothing yet")
  object DataDone extends SimplePacket("data done")
  object WillResync extends SimplePacket("will resync")

  object DatasetUpdated {
    private val label = "dataset updated"

    def apply(id: DatasetId, version: Long) = {
      val pos = labelledPacketStream(label)
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

class PacketDecodeException(msg: String) extends Exception(msg)
