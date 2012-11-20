package com.socrata.datacoordinator.util

import java.io.{Reader, Writer, IOException}
import java.util.concurrent.locks.ReentrantLock
import java.lang.Math.min

class ReaderWriterPair(bufferSize: Int) { self =>
  if(bufferSize < 2) throw new IllegalArgumentException("Buffer size must be at least 2")
  private val buf = new Array[Char](bufferSize)
  private val mutex = new ReentrantLock
  private val readAvailable = mutex.newCondition()
  private val writeAvailable = mutex.newCondition()

  private var readPtr = 0
  private var writePtr = 0

  private var readerClosed = false
  private var writerClosed = false

  override def toString = {
    mutex.lock()
    try {
      "readPtr: %d; writePtr: %d; readerClosed: %s; writerClosed: %s".format(readPtr, writePtr, readerClosed, writerClosed)
    } finally {
      mutex.unlock()
    }
  }

  val reader: Reader = new Reader {
    def read(dst: Array[Char], off: Int, len: Int): Int = {
      var dstPtr = off
      var dstRemaining = len

      mutex.lock()
      try {
        if(readerClosed) throw new IOException("reader closed")

        while(readPtr == writePtr) {
          if(writerClosed) return -1
          if(len == 0) return 0
          readAvailable.await()
        }

        val awakenWriters = if(readPtr == 0) writePtr == bufferSize - 1 else writePtr == readPtr - 1

        val copied1 = if(readPtr > writePtr) {
          val toCopy = min(dstRemaining, bufferSize - readPtr)
          System.arraycopy(buf, readPtr, dst, dstPtr, toCopy)
          dstPtr += toCopy
          dstRemaining -= toCopy

          readPtr += toCopy
          if(readPtr == bufferSize) readPtr = 0
          toCopy
        } else {
          0
        }

        val copied2 = if(dstRemaining != 0) {
          val toCopy = min(dstRemaining, writePtr - readPtr)
          System.arraycopy(buf, readPtr, dst, dstPtr, toCopy)
          readPtr += toCopy
          toCopy
        } else {
          0
        }

        if(awakenWriters) writeAvailable.signalAll()

        copied1 + copied2
      } finally {
        mutex.unlock()
      }
    }

    def close() = {
      mutex.lock()
      try {
        readerClosed = true
        writeAvailable.signalAll()
      } finally {
        mutex.unlock()
      }
    }

    override def ready = {
      mutex.lock()
      try {
        readPtr != writePtr
      } finally {
        mutex.unlock()
      }
    }
  }

  val writer: Writer = new Writer {
    def write(src: Array[Char], off: Int, len: Int) {
      var srcPtr = off
      var srcRemaining = len

      mutex.lock()
      try {
        if(writerClosed) throw new IOException("writer closed")

        def hwm = if(readPtr == 0) bufferSize - 1 else readPtr - 1

        while(srcRemaining > 0) {
          while(writePtr == hwm && !readerClosed) {
            writeAvailable.await()
          }

          if(readerClosed) {
            return
          }

          val awakenReaders = readPtr == writePtr

          if(writePtr > hwm) {
            val toCopy = min(srcRemaining, bufferSize - writePtr)
            System.arraycopy(src, srcPtr, buf, writePtr, toCopy)
            srcPtr += toCopy
            srcRemaining -= toCopy

            writePtr += toCopy
            if(writePtr == bufferSize) writePtr = 0
          } else {
            val toCopy = min(srcRemaining, hwm - writePtr)
            System.arraycopy(src, srcPtr, buf, writePtr, toCopy)
            writePtr += toCopy
            srcPtr += toCopy
            srcRemaining -= toCopy
          }

          if(awakenReaders) {
            readAvailable.signalAll()
          }
        }
      } finally {
        mutex.unlock()
      }
    }

    def flush() {}

    def close() = {
      mutex.lock()
      try {
        writerClosed = true
        readAvailable.signalAll()
      } finally {
        mutex.unlock()
      }
    }
  }
}
