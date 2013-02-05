package com.socrata.datacoordinator.packets

import java.nio.ByteBuffer

class Packet private (val buf: ByteBuffer, dummy: Int) {
  def this(buffer: ByteBuffer) = this(buffer.asReadOnlyBuffer, 0)

  assert(buf.remaining >= 4)
  assert(buf.getInt(0) == buffer.remaining(), "Size was %d; remaining is %d".format(buffer.getInt(0), buffer.remaining))
  def buffer = buf.duplicate()
  def data = buffer.position(4).asInstanceOf[ByteBuffer] /* yay no "this.type" in Java */ .slice()
}

object Packet {
  val empty = new PacketOutputStream().packet()
}
