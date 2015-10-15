package com.socrata.datacoordinator.common.util

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(bb: ByteBuffer) extends InputStream {
  def read(): Int =
    if(bb.hasRemaining) bb.get() & 0xff
    else -1

  override def read(buf: Array[Byte], offset: Int, count: Int): Int = {
    val actualCount = java.lang.Math.min(count, bb.remaining)
    if(actualCount == 0 && count != 0) {
      -1
    } else {
      bb.get(buf, offset, actualCount)
      actualCount
    }
  }

  override def available: Int = bb.remaining()

  // exactly like "available" but without the "it is never correct to use this
  // to allocate a buffer" caveat.
  def remaining: Int = bb.remaining
}
