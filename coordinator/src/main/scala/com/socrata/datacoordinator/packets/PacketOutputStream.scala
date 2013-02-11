package com.socrata.datacoordinator.packets

import java.io.OutputStream

/** A packet is a frame with the following structure:
  * [4 bytes length] [1 byte type] [length - 5 bytes data]
  */
class PacketOutputStream extends OutputStream {
  private val buffer = new UncopyingByteArrayOutputStream

  buffer.write(0); buffer.write(0); buffer.write(0); buffer.write(0) // space for the length header

  def packet(): Packet = {
    val result = buffer.buffer
    buffer.clear()
    new Packet(result.putInt(0, result.remaining).asReadOnlyBuffer())
  }

  def write(b: Int) {
    buffer.write(b)
  }

  override def write(bs: Array[Byte]) {
    buffer.write(bs)
  }

  override def write(bs: Array[Byte], offset: Int, count: Int) {
    buffer.write(bs, offset, count)
  }

  /** Size of the resulting packet, including the length header. */
  def size = buffer.size

  def dataSize = size - 4
}
