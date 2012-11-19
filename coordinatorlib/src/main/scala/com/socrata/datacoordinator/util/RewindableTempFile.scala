package com.socrata.datacoordinator.util

import java.io._
import java.nio.ByteBuffer

/** @note Not thread safe. */
class RewindableTempFile(dir: File = null, readBlockSize: Int = 1024, writeBlockSize: Int = 1024) extends Closeable { self =>
  private val file = locally {
    val filename = File.createTempFile("tmp", ".tmp", dir)
    val f = new RandomAccessFile(filename, "rw")
    if(!filename.delete()) {
      f.close()
      throw new IOException("Unable to delete temp file")
    }
    f
  }
  private var sharedPos = 0L

  def close() {
    file.close()
  }

  def rewindRead() {
    is.pos = 0
    is.markPos = 0
  }

  def rewindWrite() {
    rewindRead()
    os.pos = 0
    file.seek(0)
    file.setLength(0)
  }

  def inputStream: InputStream = is
  def outputStream: OutputStream = os

  private object is extends InputStream {
    var pos = 0L
    var markPos = 0L

    def maybeSeek() {
      if(pos != sharedPos) { file.seek(pos); sharedPos = pos }
    }

    def read() = {
      maybeSeek()
      val r = file.read()
      if(r != -1) {
        pos += 1
        sharedPos += 1
      }
      r
    }

    override def read(bs: Array[Byte], off: Int, len: Int): Int = {
      maybeSeek()
      val r = file.read(bs, off, len)
      if(r != -1) {
        pos += r
        sharedPos += r
      }
      r
    }

    override def markSupported = true
    override def mark(readLimit: Int) {
      markPos = pos
    }
    override def reset() {
      pos = markPos
    }

    override def skip(amount: Long): Long = {
      if(amount > 0) {
        maybeSeek()
        val toSkip = java.lang.Math.min(java.lang.Math.max(0, file.length - pos), amount)
        pos += toSkip
        // don't update sharedPos; this will cause a seek the next time a read happens
        toSkip
      } else {
        0
      }
    }

    override def close() {}
  }

  private object os extends OutputStream {
    var pos = 0L

    def maybeSeek() {
      if(pos != sharedPos) { file.seek(pos); sharedPos = pos }
    }

    def write(b: Int) {
      maybeSeek()
      file.write(b)
      pos += 1
      sharedPos += 1
    }

    override def write(bs: Array[Byte], off: Int, len: Int) {
      maybeSeek()
      file.write(bs, off, len)
      pos += len
      sharedPos += len
    }
  }
}
