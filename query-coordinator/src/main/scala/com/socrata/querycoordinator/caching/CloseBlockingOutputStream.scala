package com.socrata.querycoordinator.caching

import java.io.OutputStream

class CloseBlockingOutputStream(underlying: OutputStream) extends OutputStream {
  override def write(b: Int) = underlying.write(b)
  override def write(b: Array[Byte]) = underlying.write(b)
  override def write(b: Array[Byte], off: Int, len: Int) = underlying.write(b, off, len)
  override def flush() = underlying.flush()

  override def close() {}
}
