package com.socrata.datacoordinator.packets

import java.nio.ByteBuffer

class Packet private (val buf: ByteBuffer, dummy: Int) {
  def this(buffer: ByteBuffer) = this(buffer.asReadOnlyBuffer, 0)

  require(buf.remaining >= 4)
  require(buf.getInt(0) == buffer.remaining(), "Size was %d; remaining is %d".format(buffer.getInt(0), buffer.remaining))
  def buffer = buf.duplicate()
  def data = buffer.position(4).asInstanceOf[ByteBuffer] /* yay no "this.type" in Java */ .slice()
  def dataSize = buf.remaining - 4

  override def equals(o: Any) = o match {
    case that: Packet =>
      this.buf == that.buf
    case _ =>
      false
  }
}

object Packet {
  val empty = new PacketOutputStream().packet()

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

  def labelledPacketStream(s: String): PacketOutputStream = {
    val pos = new PacketOutputStream
    var i = 0
    while(i != s.length) {
      pos.write(s.charAt(i).toByte)
      i += 1
    }
    pos.write(labelSep.toByte)
    pos
  }
}
