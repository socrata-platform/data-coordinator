package com.socrata.querycoordinator.caching

import java.io.{IOException, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class FileChannelInputStream(fc: FileChannel) extends InputStream {
  private var pos = 0L
  private var markPos = -1L
  private var markLimit = 0

  override def read(): Int = {
    val scratch = new Array[Byte](1)
    read(scratch) match {
      case -1 =>
        -1
      case 1 =>
        scratch(0) & 0xff
    }
  }

  override def read(bs: Array[Byte], off: Int, len: Int): Int = synchronized {
    val buf = ByteBuffer.wrap(bs, off, len)
    fc.read(buf, pos) match {
      case -1 => -1
      case n => pos += n; n
    }
  }

  override def available() = synchronized {
    val remaining = (fc.size - pos) max 0L
    if(remaining > Int.MaxValue) Int.MaxValue
    else remaining.toInt
  }

  override def markSupported = true

  override def mark(readLimit: Int) = synchronized {
    markPos = pos
    markLimit = readLimit
  }

  override def reset(): Unit = synchronized {
    if(markPos >= 0 && (pos - markPos <= markLimit)) {
      pos = markPos
    } else {
      throw new IOException("Invalid mark")
    }
  }

  override def skip(n: Long): Long = synchronized {
    val remaining = (fc.size - pos) max 0L
    val toSkip = remaining min n
    if(toSkip <= 0) return 0
    pos += toSkip
    toSkip
  }

  override def close(): Unit = {
    fc.close()
  }
}
