package com.socrata.datacoordinator.packets

import java.nio.ByteBuffer

class PacketAccumulator(val maxPacketSize: Int) {
  private var stream = new UncopyingByteArrayOutputStream
  private var tmp: Array[Byte] = _
  private var expectedSize = -1

  def writeIntoStream(buf: ByteBuffer, count: Int) {
    require(count >= 0 && count <= buf.remaining, "count is out of bounds")
    if(buf.hasArray) {
      stream.write(buf.array, buf.arrayOffset + buf.position, count)
      buf.position(buf.position + count)
    } else {
      if(tmp == null) tmp = new Array[Byte](1000)

      var remaining = count
      while(remaining > 0) {
        val thisPass = java.lang.Math.min(remaining, tmp.length)
        buf.get(tmp, 0, thisPass)
        stream.write(tmp, 0, thisPass)
        remaining -= thisPass
      }
    }
  }

  /** Accumulates bytes into a packet.  If a full packet is built, returns it.
    * In that case, the byte buffer might not be fully empty.
    * @throws BadPacketSize if the packet's size header claims to be smaller than 5 or larger than `maxPacketSize`.
    */
  def accumulate(bb: ByteBuffer): Option[Packet] = {
    if(stream.size < 4) { // packet size not fully received yet
    val bytesForSize = java.lang.Math.min(bb.remaining, 4 - stream.size)
      writeIntoStream(bb, bytesForSize)
      if(stream.size == 4) {
        expectedSize = decodeBigEndianInt(stream.underlyingByteArray)
        if(expectedSize < 5 || expectedSize > maxPacketSize)
          throw new BadPacketSize(expectedSize)
      }
    }
    if(stream.size >= 4 && bb.hasRemaining) {
      val remainingBytesForPacket = java.lang.Math.min(expectedSize - stream.size, bb.remaining)
      writeIntoStream(bb, remainingBytesForPacket)

      if(stream.size == expectedSize) {
        val result = new Packet(stream.buffer)
        stream = new UncopyingByteArrayOutputStream
        expectedSize = -1
        return Some(result)
      }
    }
    None
  }

  def partial = stream.size != 0

  def decodeBigEndianInt(bytes: Array[Byte]): Int =
    bytes(0) << 24 | ((bytes(1) & 0xff) << 16) | ((bytes(2) & 0xff) << 8) | (bytes(3) & 0xff)
}
