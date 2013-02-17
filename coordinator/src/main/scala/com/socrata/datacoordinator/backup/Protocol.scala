package com.socrata.datacoordinator.backup

import java.io.{InputStreamReader, OutputStreamWriter, DataInputStream, DataOutputStream}

import com.rojoma.json.util.JsonUtil

import com.socrata.datacoordinator.packets.Packet
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.common.util.ByteBufferInputStream
import com.socrata.datacoordinator.truth.metadata.{UnanchoredDatasetInfo, UnanchoredColumnInfo, UnanchoredCopyInfo, DatasetInfo}

class Protocol[LogData](logDataCodec: Codec[LogData]) {
  import Packet.{SimplePacket, LabelledPacket}

  // primary -> backup
  object NothingYet extends SimplePacket("nothing yet")
  object DataDone extends SimplePacket("data done")

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

  // Resync flow:
  //    Backup sends "resync required"
  //    Primary sends "will resync" (eventually; backup must discard packets until this is received)
  //    Backup sends 0 or more "preparing for resync" as it drops tables
  //    Backup sends "Awaiting next copy"
  //    Primary sends "next resync copy"      \
  //    Primary streams CSV                    } Repeated 1 or more times; middle steps occur only for non-discarded copies
  //    Backup sends "Awaiting next copy"     /
  //    Primary sends "no more copies"
  //    Backup commits and sends "resync complete"
  // Resyncing; primary -> backup
  object WillResync extends LabelledPacket("resync/dataset") {
    def apply(id: UnanchoredDatasetInfo) =
      create { os =>
        val w = new OutputStreamWriter(os, "UTF-8")
        JsonUtil.writeJson(w, id)
        w.flush()
      }
    def unapply(packet: Packet): Option[UnanchoredDatasetInfo] =
      extract(packet) map { data =>
        val r = new InputStreamReader(new ByteBufferInputStream(data), "UTF-8")
        JsonUtil.readJson[UnanchoredDatasetInfo](r).getOrElse {
          throw new PacketDecodeException("resyncing dataset packet does not contain a datasetinfo object")
        }
      }
  }

  val ResyncStreamDataLabel = "d"
  val ResyncStreamEndLabel = "end of stream"

  object NextResyncCopy extends LabelledPacket("resync/copy") {
    // this schema has to be a Seq; it reflects the order of columns in the following CSV.
    def apply(id: UnanchoredCopyInfo, schema: Seq[UnanchoredColumnInfo]) =
      create { os =>
        val w = new OutputStreamWriter(os, "UTF-8")
        JsonUtil.writeJson(w, id)
        JsonUtil.writeJson(w, schema)
        w.flush()
      }
    def unapply(packet: Packet): Option[(UnanchoredCopyInfo, Seq[UnanchoredColumnInfo])] =
      extract(packet) map { data =>
        val r = new InputStreamReader(new ByteBufferInputStream(data), "UTF-8")
        val copyInfo = JsonUtil.readJson[UnanchoredCopyInfo](r).getOrElse {
          throw new PacketDecodeException("resyncing copy packet does not contain a copyinfo object")
        }
        val columns = JsonUtil.readJson[Vector[UnanchoredColumnInfo]](r).getOrElse {
          throw new PacketDecodeException("resyncing copy packet does not contain a list of columns")
        }
        (copyInfo, columns)
      }
  }

  object NoMoreCopies extends SimplePacket("resync/no more copies")

  // Resyncing; backup -> primary
  object PreparingDatabaseForResync extends SimplePacket("resync/preparing database")
  object AwaitingNextCopy extends SimplePacket("resync/awaiting copy")
  object ResyncComplete extends SimplePacket("resync/complete")
}

class PacketDecodeException(msg: String) extends Exception(msg)
