package com.socrata.datacoordinator.backup

import java.io.{DataInputStream, DataOutputStream}

import com.socrata.datacoordinator.packets.Packet
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.common.util.ByteBufferInputStream

class Protocol[LogData](logDataCodec: Codec[LogData]) {
  import Packet.{SimplePacket, LabelledPacket}

  // primary -> backup
  object NothingYet extends SimplePacket("nothing yet")
  object DataDone extends SimplePacket("data done")
  object WillResync extends SimplePacket("will resync")

  object DatasetUpdated extends LabelledPacket("dataset updated") {
    def apply(id: DatasetId, version: Long) =
      create { os =>
        val dos = new DataOutputStream(os)
        dos.writeLong(id.underlying)
        dos.writeLong(version)
        dos.flush()
      }

    def unapply(packet: Packet): Option[(DatasetId, Long)] =
      extract(packet) map { data =>
        if(data.remaining != 16) throw new PacketDecodeException("DatasetUpdate packet does not contain 16 bytes of payload")
        val id = new DatasetId(data.getLong())
        val version = data.getLong()
        (id, version)
      }
  }

  object LogData extends LabelledPacket("log data") {
    def apply(event: LogData) = create { os =>
      val dos = new DataOutputStream(os)
      dos.write("log data|".getBytes)
      logDataCodec.encode(dos, event)
      dos.flush()
    }

    def unapply(packet: Packet): Option[LogData] = extract(packet) map { data =>
      val stream = new DataInputStream(new ByteBufferInputStream(data))
      logDataCodec.decode(stream)
    }
  }

  // backup -> primary
  object OkStillWaiting extends SimplePacket("ok still waiting")
  object AlreadyHaveThat extends SimplePacket("already have that")
  object ResyncRequired extends SimplePacket("resync required")
  object WillingToAccept extends SimplePacket("willing to accept")
  object AcknowledgeReceipt extends SimplePacket("acknowledged")

  val StreamDataLabel = "d"
  val StreamEndLabel = "end of stream"
}

class PacketDecodeException(msg: String) extends Exception(msg)
