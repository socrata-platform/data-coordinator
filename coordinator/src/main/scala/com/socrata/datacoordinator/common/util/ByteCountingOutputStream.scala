package com.socrata.datacoordinator.common.util

import java.io.OutputStream

class ByteCountingOutputStream(underlying: OutputStream) extends OutputStream {
  private var count = 0L

  def bytesWritten = count

  def write(b: Int) {
    underlying.write(b)
    count += 1
  }

  override def write(bs: Array[Byte], offset: Int, length: Int) {
    underlying.write(bs, offset, length)
    count += length
  }

  override def flush() {
    underlying.flush()
  }

  override def close() {
    underlying.close()
  }
}
