package com.socrata.querycoordinator.caching

import java.io.{IOException, OutputStream}

abstract class ChunkingOutputStream(size: Int) extends OutputStream {
  require(size > 0, "size must be positive")

  private var closed = false
  private var buf = new Array[Byte](size)
  private var endPtr = 0

  private def ensureNotClosed(): Unit =
    if(closed) throw new IOException("Write to closed stream")

  def onChunk(bytes: Array[Byte])

  def write(b: Int) = synchronized {
    ensureNotClosed()
    buf(endPtr) = b.toByte
    endPtr += 1
    if(endPtr == size) {
      endPtr = 0
      onChunk(buf)
    }
  }

  override def write(bs: Array[Byte]): Unit =
    write(bs, 0, bs.length)

  override def write(bs: Array[Byte], off: Int, len: Int): Unit = synchronized {
    ensureNotClosed()
    def loop(off: Int, len: Int): Unit = {
      if(len > 0) {
        val space = size - endPtr
        if(len < space) {
          System.arraycopy(bs, off, buf, endPtr, len)
          endPtr += len
        } else {
          System.arraycopy(bs, off, buf, endPtr, space)
          onChunk(buf)
          buf = new Array[Byte](size) // the previous line gives the buffer away; we need a new one
          endPtr = 0
          loop(off + space, len - space)
        }
      }
    }
    loop(off, len)
  }

  override def close(): Unit = synchronized {
    if(!closed) {
      closed = true
      if(endPtr != 0) {
        val tmp = buf.take(endPtr)
        endPtr = 0
        onChunk(tmp)
      }
    }
  }
}
