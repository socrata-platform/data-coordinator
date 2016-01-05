package com.socrata.querycoordinator.caching

import java.io.InputStream

class CloseBlockingInputStream(underlying: InputStream) extends InputStream {
  override def read() = underlying.read()
  override def read(bs: Array[Byte]) = underlying.read(bs)
  override def read(bs: Array[Byte], off: Int, len: Int) = underlying.read(bs, off, len)
  override def available() = underlying.available()
  override def skip(n: Long) = underlying.skip(n)
  override def markSupported = underlying.markSupported
  override def mark(limit: Int) = underlying.mark(limit)
  override def reset() = underlying.reset()

  override def close() {}
}
