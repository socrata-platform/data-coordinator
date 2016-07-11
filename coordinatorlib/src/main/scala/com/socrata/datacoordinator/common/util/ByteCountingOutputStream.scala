package com.socrata.datacoordinator.common.util

import java.io.OutputStream

class ByteCountingOutputStream(underlying: OutputStream) extends OutputStream {
  private var count = 0L

  def bytesWritten: Long = count

  def write(b: Int): Unit = {
    underlying.write(b)
    count += 1
  }

  override def write(bs: Array[Byte], offset: Int, length: Int): Unit = {
    underlying.write(bs, offset, length)
    count += length
  }

  override def flush(): Unit = {
    underlying.flush()
  }

  override def close(): Unit = {
    underlying.close()
  }
}
