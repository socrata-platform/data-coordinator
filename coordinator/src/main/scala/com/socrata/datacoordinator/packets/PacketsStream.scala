package com.socrata.datacoordinator.packets

import java.io.{InputStream, OutputStream}
import scala.concurrent.duration.Duration
import java.nio.ByteBuffer

object PacketsStream {
  object End extends Packet.SimplePacket("end-stream")
  val streamLabel = "stream-data"
}

class PacketsInputStream(packets: Packets, readTimeout: Duration = Duration.Inf) extends InputStream {
  private var currentBuffer = receive()

  def read(): Int =
    if(currentBuffer == null) -1
    else {
      val result = currentBuffer.get() & 0xff
      maybeReceive()
      result
    }

  override def read(bs: Array[Byte], offset: Int, length: Int): Int =
    if(currentBuffer == null) -1
    else {
      val result = length.min(currentBuffer.remaining())
      currentBuffer.get(bs, offset, result)
      maybeReceive()
      result
    }

  private def maybeReceive() {
    if(!currentBuffer.hasRemaining) currentBuffer = receive()
  }

  private def receive(): ByteBuffer = {
    packets.receive(readTimeout) match {
      case Some(PacketsStream.End()) =>
        null
      case Some(packet) =>
        Packet.packetLabelled(packet, PacketsStream.streamLabel) match {
          case Some(data) =>
            data
          case None =>
            sys.error("Not a stream packet?") // TODO: Better error
        }
      case None =>
        sys.error("End of input received?") // TODO: Better error
    }
  }
}

object PacketsInputStream {
  def isStreamPacket(p: Packet) = p.dataSize > 0 && {
    val b = p.data
    b.get(0) == 0 || b.get(0) == 1
  }
}

class PacketsOutputStream(packets: Packets, targetPacketSize: Int, writeTimeout: Duration = Duration.Inf) extends OutputStream {
  require(targetPacketSize >= PacketsOutputStream.minimumSize, "Target packet size not large enough for the header plus one byte")
  private var currentPacket: PacketOutputStream = null

  def freshPacket() = Packet.labelledPacketStream(PacketsStream.streamLabel)

  def write(b: Int) {
    if(currentPacket == null) currentPacket = freshPacket()
    currentPacket.write(b)
    maybeFlush()
  }

  override def write(bs: Array[Byte], offset: Int, length: Int) {
    var hd = offset
    var rem = length
    while(rem > 0) {
      if(currentPacket == null) currentPacket = freshPacket()
      val toWrite = rem.min(targetPacketSize - currentPacket.size)
      currentPacket.write(bs, hd, toWrite)
      maybeFlush()
      hd += toWrite
      rem -= toWrite
    }
  }

  private def maybeFlush() {
    if(currentPacket.size >= targetPacketSize) flush()
  }

  override def flush() {
    if(currentPacket != null) {
      packets.send(currentPacket.packet(), writeTimeout)
      postWrite()
      currentPacket = null
    }
  }

  override def close() {
    flush()
    packets.send(PacketsStream.End(), writeTimeout)
    postWrite()
  }

  // overridable
  def postWrite() {}
}

object PacketsOutputStream {
  val minimumSize = locally {
    val pos = Packet.labelledPacketStream(PacketsStream.streamLabel)
    pos.write(1)
    pos.size
  }
}
