package com.socrata.datacoordinator.util

import java.io.{Reader, Writer, IOException}
import java.util.concurrent.locks.ReentrantLock

private[util] class RWSharedState(val bufferSize: Int, val blockCount: Int) {
  import RWSharedState._

  if(bufferSize < 1) throw new IllegalArgumentException("bufferSize must be at least 1")
  if(blockCount < 1) throw new IllegalArgumentException("blockCount must be at least 1")

  val queue = new java.util.ArrayDeque[Message]

  var readerClosed = false

  val mutex = new ReentrantLock
  val readAvailable = mutex.newCondition()
  val writeAvailable = mutex.newCondition()
}

private[util] object RWSharedState {
  sealed abstract class Message
  class Block(val buf: Array[Char], val endPtr: Int) extends Message {
    var readPtr = 0
    def available = endPtr - readPtr
  }
  case object EndSentinel extends Message
}

private[util] class RWReader(sharedState: RWSharedState) extends Reader {
  import sharedState._
  import RWSharedState._

  var closed = false
  var lastMessage: Message = null

  def read(dst: Array[Char], off: Int, len: Int): Int = {
    if(closed) throw new IOException("reader already closed")

    val message =
      if(lastMessage != null) {
        lastMessage
      } else {
        var msg: Message = null
        mutex.lock()
        try {
          while(queue.isEmpty) readAvailable.await()
          msg = queue.removeFirst()
          if(queue.size == blockCount - 1) writeAvailable.signal()
        } finally {
          mutex.unlock()
        }
        lastMessage = msg
        msg
      }

    message match {
      case block: Block =>
        val toCopy = java.lang.Math.min(len, block.available)
        System.arraycopy(block.buf, block.readPtr, dst, off, toCopy)
        block.readPtr += toCopy
        if(block.readPtr == block.endPtr) lastMessage = null
        toCopy
      case EndSentinel =>
        -1
    }
  }

  def close() {
    mutex.lock()
    try {
      readerClosed = true
      writeAvailable.signal()
    } finally {
      mutex.unlock()
    }

    closed = true
  }

  override def ready = {
    if(closed || lastMessage != null) true
    else {
      mutex.lock()
      try {
        if(!queue.isEmpty) lastMessage = queue.removeFirst()
      } finally {
        mutex.unlock()
      }
      lastMessage != null
    }
  }
}

class RWWriter(sharedState: RWSharedState) extends Writer {
  import sharedState._
  import RWSharedState._

  var currentBuffer: Array[Char] = null
  var writePtr = 0
  var closed = false

  def testOpen() {
    if(closed) throw new IOException("Writer already closed")
  }

  override def write(s: String, off: Int, len: Int) {
    var ptr = off
    var remainingData = len

    testOpen()

    while(remainingData > 0) {
      if(currentBuffer == null) {
        currentBuffer = new Array[Char](bufferSize)
        writePtr = 0
      }

      val remainingSpace = bufferSize - writePtr
      var toCopy = math.min(remainingSpace, remainingData)
      val cb = currentBuffer
      var i = 0
      var wP = writePtr
      while(i != toCopy) {
        cb(wP) = s.charAt(ptr + i)
        wP += 1
        i += 1
      }
      writePtr += toCopy
      if(toCopy == remainingSpace) doFlush()

      ptr += toCopy
      remainingData -= toCopy
    }
  }

  def write(src: Array[Char], off: Int, len: Int) {
    var ptr = off
    var remainingData = len

    testOpen()

    while(remainingData > 0) {
      if(currentBuffer == null) {
        currentBuffer = new Array[Char](bufferSize)
        writePtr = 0
      }

      val remainingSpace = bufferSize - writePtr
      val toCopy = math.min(remainingSpace, remainingData)
      System.arraycopy(src, ptr, currentBuffer, writePtr, toCopy)
      writePtr += toCopy
      if(toCopy == remainingSpace) doFlush()

      ptr += toCopy
      remainingData -= toCopy
    }
  }

  def flush() {
    testOpen()
    if(currentBuffer == null) return

    doFlush()
  }

  def doFlush() {
    mutex.lock()
    try {
      while(!readerClosed && queue.size >= blockCount) {
        writeAvailable.await()
      }

      if(readerClosed) throw new IOException("Writer closed")

      queue.addLast(new Block(currentBuffer, writePtr))
      if(queue.size == 1) readAvailable.signal()
    } finally {
      mutex.unlock()
    }

    currentBuffer = null
    writePtr = 0
  }

  def close() {
    if(!closed) {
      mutex.lock()
      try {
        if(!readerClosed && currentBuffer != null) doFlush()
        queue.add(EndSentinel)
        readAvailable.signal()
      } finally {
        mutex.unlock()
      }
      closed = true
    }
  }

  override protected def finalize() {
    close()
  }
}

class ReaderWriterPair(bufferSize: Int, blockCount: Int) { self =>
  val (reader: Reader, writer: Writer) = locally {
    val sharedState = new RWSharedState(bufferSize, blockCount)
    (new RWReader(sharedState), new RWWriter(sharedState))
  }
}
