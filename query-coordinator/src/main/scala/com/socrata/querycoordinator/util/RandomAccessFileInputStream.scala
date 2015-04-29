package com.socrata.querycoordinator.util

import java.io.{InputStream, RandomAccessFile}

class RandomAccessFileInputStream(underlying: RandomAccessFile) extends InputStream {
  private[this] var markPos = underlying.getFilePointer

  def read(): Int = underlying.read()
  override def read(buf: Array[Byte], offset: Int, len: Int): Int = underlying.read(buf, offset, len)

  override def skip(n: Long): Long = {
    if (n >= 0) {
      val p = underlying.getFilePointer
      val trueSkip = Math.min(underlying.length - p, n)
      underlying.seek(p + trueSkip)
      trueSkip
    } else {
      0
    }
  }

  override def close(): Unit = underlying.close()
  override def markSupported: Boolean = true
  override def mark(x: Int): Unit = markPos = underlying.getFilePointer
  override def reset(): Unit = underlying.seek(markPos)
}
