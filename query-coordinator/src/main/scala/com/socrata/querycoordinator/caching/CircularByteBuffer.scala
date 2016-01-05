package com.socrata.querycoordinator.caching

import java.io.{InputStream, OutputStream}

object CircularByteBuffer {
  def splitData(split: Int, space: Int, data: Array[Byte]) = {
    require(split >= 0, "Out of bounds split")
    require(split <= data.length, "Out of bounds split")
    require(space >= 0, "Space must be non-negative")
    val buf = new CircularByteBuffer(space + data.length)
    // data[0:split] is at the end of the array, data[split:] is at the start
    System.arraycopy(data, split, buf.buffer, 0, data.length - split)
    System.arraycopy(data, 0, buf.buffer, space + 1 + data.length - split , split)
    buf.readPtr = space + 1 + data.length - split
    buf.writePtr = data.length - split
    buf
  }

  def splitSpace(preSpace: Int, data: Array[Byte], postSpace: Int) = {
    require(preSpace >= 0, "Pre-space must be non-negative")
    require(postSpace >= 0, "Post-space must be non-negative")
    val buf = new CircularByteBuffer(preSpace + data.length + postSpace)
    System.arraycopy(data, 0, buf.buffer, preSpace, data.length)
    buf.readPtr = preSpace
    buf.writePtr = preSpace + data.length
    buf
  }
}

// not thread safe; concurrent access must be protected by a lock
final class CircularByteBuffer(val capacity: Int) {
  require(capacity >= 0, "Capactiy must be non-negative")
  require(capacity < Int.MaxValue, "Capacity must be less than " + Int.MaxValue)
  private val size = capacity + 1
  private val buffer = new Array[Byte](size)
  private var writePtr = 0
  private var readPtr = 0

  override def toString: String = {
    val s = new StringBuilder
    s += '['
    var i = readPtr
    var didOne = false
    while(i != writePtr) {
      if(didOne) s += ',' else didOne = true
      s.append(buffer(i))
      i = (i + 1) % size
    }
    s += ']'
    s.toString
  }

  def available: Int =
    if(readPtr <= writePtr) writePtr - readPtr
    else writePtr + size - readPtr

  def space: Int =
    if(writePtr < readPtr) readPtr - writePtr - 1
    else if(readPtr == 0) size - writePtr - 1
    else (size - writePtr) + (readPtr - 1)

  def spaceAhead =
    if(writePtr < readPtr) readPtr - writePtr - 1
    else if(readPtr == 0) size - writePtr - 1
    else size - writePtr

  private def readAsMuchAsPossible(in: InputStream, buffer: Array[Byte], offset: Int, count: Int): Int = {
    def loop(offset: Int, count: Int, soFar: Int): Int = {
      if(count == 0) soFar
      else in.read(buffer, offset, count) match {
        case -1 => soFar
        case n => loop(offset + n, count - n, soFar + n)
      }
    }
    in.read(buffer, offset, count) match {
      case -1 => -1
      case n => loop(offset + n, count - n, n)
    }
  }

  def read(in: InputStream, len: Int): Int = {
    val block = spaceAhead
    if(block == 0) return 0
    val toCopy = block min len
    readAsMuchAsPossible(in, buffer, writePtr, toCopy) match {
      case -1 => -1
      case n =>
        val x = writePtr + n
        if(x == size) {
          // wraparound
          writePtr = 0
          if(toCopy != len) {
            read(in, len - n) match {
              case -1 => n
              case m => n + m
            }
          } else {
            n
          }
        } else {
          writePtr = x
          n
        }
    }
  }

  def put(bs: Array[Byte]): Int = put(bs, 0, bs.length)

  def put(bs: Array[Byte], offset: Int, len: Int): Int = {
    val block = spaceAhead
    if(block == 0) return 0
    val toCopy = block min len
    System.arraycopy(bs, offset, buffer, writePtr, toCopy)
    val x = writePtr + toCopy
    if(x == size) {
      // wraparound
      writePtr = 0
      if(toCopy != len) {
        toCopy + put(bs, offset + toCopy, len - toCopy)
      } else {
        toCopy
      }
    } else {
      writePtr = x
      toCopy
    }
  }

  def peek(bs: Array[Byte], offset: Int, len: Int): Int = {
    if(readPtr == writePtr) return -1

    if(readPtr < writePtr) {
      val trueLen = (writePtr - readPtr) min len
      System.arraycopy(buffer, readPtr, bs, offset, trueLen)
      trueLen
    } else {
      val blockA = size - readPtr
      if(blockA >= len) {
        System.arraycopy(buffer, readPtr, bs, offset, len)
        len
      } else {
        System.arraycopy(buffer, readPtr, bs, offset, blockA)
        val remainder = writePtr min (len - blockA)
        System.arraycopy(buffer, 0, bs, offset + blockA, remainder)
        blockA + remainder
      }
    }
  }

  def get(bs: Array[Byte], offset: Int, len: Int): Int = {
    peek(bs, offset, len) match {
      case -1 =>
        -1
      case n =>
        val x = readPtr + n
        readPtr = if(x >= size) x - size else x
        n
    }
  }

  def get(bs: Array[Byte]): Int = get(bs, 0, bs.length)

  def getAll: Array[Byte] = {
    val bs = new Array[Byte](available)
    get(bs)
    bs
  }

  def writeTo(w: OutputStream): Int = {
    if(readPtr != writePtr) {
      val copied =
        if(readPtr < writePtr) {
          val toCopy = writePtr - readPtr
          w.write(buffer, readPtr, toCopy)
          toCopy
        } else {
          val toCopy0 = size - readPtr
          w.write(buffer, readPtr, toCopy0)
          if(writePtr != 0) w.write(buffer, 0, writePtr)
          toCopy0 + writePtr
        }
      clear()
      copied
    } else {
      0
    }
  }

  def clear() {
    readPtr = 0
    writePtr = 0
  }

  def clearAt(n: Int) {
    val t = if(n == Int.MinValue) 0 else n.abs % size
    readPtr = t
    writePtr = t
  }
}
