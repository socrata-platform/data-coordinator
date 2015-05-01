package com.socrata.querycoordinator.util

import java.io._

/**
 * @note unlike most streams, this does NOT take ownership of the underlying stream.
 */
// TODO: configurable default memory buffer size
class TeeToTempInputStream(underlying: InputStream,
                           inMemoryBufferSize: Int = 10240, // scalastyle:ignore magic.number
                           tempDir: File = new File(sys.props("java.io.tmpdir"))) extends InputStream {
  private[this] val inMemoryBuffer = new Array[Byte](inMemoryBufferSize)
  private[this] var inMemoryBufferPtr = 0
  private[this] var tempFile: RandomAccessFile = null // scalastyle:ignore null
  private[this] var restreamed = false
  private[this] val restreamReadErrorMessage = "Cannot read after restream has been called"

  private def augmentBuffer(c: Byte): Unit = {
    if (inMemoryBufferPtr == inMemoryBufferSize) flushTempFile()
    inMemoryBuffer(inMemoryBufferPtr) = c
    inMemoryBufferPtr += 1
  }

  private def augmentBuffer(buf: Array[Byte], offset: Int, len: Int): Unit = {
    if (len <= inMemoryBufferSize - inMemoryBufferPtr) {
      System.arraycopy(buf, offset, inMemoryBuffer, inMemoryBufferPtr, len)
      inMemoryBufferPtr += len
    } else if (len > inMemoryBufferSize) {
      flushTempFile()
      tempFile.write(buf, offset, len)
    } else {
      flushTempFile()
      System.arraycopy(buf, offset, inMemoryBuffer, 0, len)
      inMemoryBufferPtr = len
    }
  }

  private def openTempFile(): Unit = {
    assert(Option(tempFile).isEmpty)
    val fileName = File.createTempFile("tee", ".tmp", tempDir)
    try {
      tempFile = new RandomAccessFile(fileName, "rw")
    } finally {
      fileName.delete()
    }
  }

  private def flushTempFile(): Unit = {
    if (Option(tempFile).isEmpty) openTempFile()
    tempFile.write(inMemoryBuffer, 0, inMemoryBufferPtr)
    inMemoryBufferPtr = 0
  }

  def read(): Int = {
    if (restreamed) throw new IllegalStateException(restreamReadErrorMessage)
    underlying.read() match {
      case -1 => -1
      case c: Int => augmentBuffer(c.toByte); c
    }
  }

  override def read(buf: Array[Byte], offset: Int, length: Int): Int = {
    if (restreamed) throw new IllegalStateException(restreamReadErrorMessage)
    underlying.read(buf, offset, length) match {
      case -1 => -1
      case n: Int => augmentBuffer(buf, offset, n); n
    }
  }

  def restream(): InputStream = {
    if (restreamed) throw new IllegalStateException("Can only restream once")
    restreamed = true
    if (Option(tempFile).isEmpty) {
      new ByteArrayInputStream(inMemoryBuffer, 0, inMemoryBufferPtr)
    } else {
      flushTempFile()
      tempFile.seek(0)
      new BufferedInputStream(new RandomAccessFileInputStream(tempFile))
    }
  }

  override def close(): Unit = {
    if (Option(tempFile).nonEmpty && !restreamed) tempFile.close()
  }
}
