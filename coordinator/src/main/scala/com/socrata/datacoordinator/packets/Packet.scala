package com.socrata.datacoordinator.packets

import java.nio.ByteBuffer
import java.io.{ByteArrayOutputStream, OutputStream}

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

  private def simpleWrite(o: OutputStream, s: String) {
    var i = 0
    while(i != s.length) {
      o.write(s.charAt(i).toByte)
      i += 1
    }
  }

  /** A `SimplePacket` is a packet which carries no data, only a label.
    * The label must consist entirely of latin-1 characters, excluding
    * NUL. */
  class SimplePacket(s: String) {
    val data = locally {
      val pos = new PacketOutputStream
      simpleWrite(pos, s)
      pos.packet()
    }

    def apply() = data
    def unapply(p: Packet): Boolean = p == data
  }

  /** A `LabelledPacket` is a packet which carries a label together with some
    * amount of data. The label must consist entirely of latin-1 characters,
    * excluding NUL. */
  class LabelledPacket(prefixStr: String) {
    private def labelSep = '\0'.toByte

    private val prefix = locally {
      val baos = new ByteArrayOutputStream(prefixStr.length + 1)
      simpleWrite(baos, prefixStr)
      baos.write(labelSep)
      baos.toByteArray
    }

    @inline
    protected final def create(f: OutputStream => Any): Packet = {
      val pos = packetOutputStream()
      f(pos)
      pos.packet()
    }

    protected def packetOutputStream() = {
      val pos = new PacketOutputStream()
      pos.write(prefix)
      pos
    }

    protected final def extract(packet: Packet): Option[ByteBuffer] = {
      if(packet.dataSize >= prefix.length) {
        val data = packet.data
        var i = 0
        while(i != prefix.length && data.get() == prefix(i)) { i += 1 }
        if(i == prefix.length) Some(data.slice())
        else None
      } else {
        None
      }
    }
  }

  /** A `SimpleLabelledPacket` is a packet which carries a label together with a
    * binary blob. The label must consist entirely of latin-1 characters,
    * excluding NUL. */
  class SimpleLabelledPacket(prefixStr: String) extends LabelledPacket(prefixStr) {
    @inline final def apply(f: OutputStream => Any) = create(f)
    @inline final def unapply(packet: Packet) = extract(packet)

    override def packetOutputStream() = super.packetOutputStream()
  }
}
