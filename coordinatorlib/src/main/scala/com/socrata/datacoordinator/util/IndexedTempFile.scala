package com.socrata.datacoordinator.util

import java.io._
import java.nio.ByteBuffer
import scala.annotation.tailrec

class BlockBasedRandomAccessFile(blockSizeShift: Int, file: RandomAccessFile, fillHoles: Boolean = BlockBasedRandomAccessFile.fillHoles) extends Closeable {
  val blockSize = 1 << blockSizeShift
  val block = new Array[Byte](blockSize)
  val asBuffer = ByteBuffer.wrap(block)

  private[this] val io = file.getChannel()
  private[this] val asWriteBuffer = asBuffer.duplicate()
  private[this] var currentBlock = 0L
  private[this] var knownLength = io.size()
  private[this] var readCount = 0L
  private[this] var writeCount = 0L

  def reads = readCount
  def writes = writeCount

  def close() {
    file.close()
  }

  def blockNumber = currentBlock

  def nextBlock() {
    readBlock(currentBlock + 1)
  }

  def blockPosition = currentBlock << blockSizeShift

  def readBlock(num: Long) {
    val offset = num << blockSizeShift
    asBuffer.clear()
    val count = Math.max(0, io.read(asBuffer, offset))
    java.util.Arrays.fill(block, count, blockSize, 0.toByte)
    asBuffer.clear()
    currentBlock = num
    readCount += 1
  }

  def writeBlock() {
    val offset = currentBlock << blockSizeShift
    if(offset > knownLength && fillHoles) fillTo(offset)
    asWriteBuffer.clear()
    io.write(asWriteBuffer, offset)
    knownLength = Math.max(offset + blockSize, knownLength)
    writeCount += 1
  }

  private def fillTo(offset: Long) {
    // "If the given position is greater than the file's current size then the file
    // will be grown to accommodate the new bytes; the values of any bytes between
    // the previous end-of-file and the newly-written bytes are unspecified."
    //     -- FileChannel#write(ByteBuffer, Long)'s javadoc
    //
    // In practice I think they'll be 0, particularly on linux (possibly all POSIX
    // systems? see lseek(2)) which is why this is optional.
    val empty = ByteBuffer.allocate(1024)
    var pos = io.size()
    while(pos < offset) {
      empty.position(0).limit(Math.min(1024L, offset - pos).toInt)
      pos += io.write(empty, pos)
    }
  }
}

object BlockBasedRandomAccessFile {
  val fillHoles = System.getProperty("os.name") != "Linux"
}

case class IndexedTempFileStats(indexReads: Long, indexWrites: Long, nonLinearIndexSeeks: Long, dataReads: Long, dataWrites: Long, nonLinearDataSeeks: Long)

/** A file of records, optimized for "mostly linear" reading and writing */
class IndexedTempFile(indexBufSizeHint: Int, dataBufSizeHint: Int, tmpDir: File = new File(System.getProperty("java.io.tmpdir"))) extends Closeable {
  require(indexBufSizeHint > 0, "indexBufSizeHint")
  require(dataBufSizeHint > 0, "dataBufSizeHine")

  private def nearestPowerOfTwo(x: Int): Int = {
    val lowerBoundPoT = 31 - java.lang.Integer.numberOfLeadingZeros(x)
    if(lowerBoundPoT == 31) lowerBoundPoT
    else if(lowerBoundPoT == -1) 0
    else {
      val nextPoT = lowerBoundPoT + 1
      if((1 << nextPoT) - x <= x - (1 << lowerBoundPoT)) nextPoT
      else lowerBoundPoT
    }
  }

  private[this] val indexBufSizeShift = Math.max(4, nearestPowerOfTwo(indexBufSizeHint)) // always a multiple of 16
  private[this] val dataBufSizeShift = nearestPowerOfTwo(dataBufSizeHint)

  private[this] val indexBufSize = 1 << indexBufSizeShift
  private[this] val dataBufSize = 1 << dataBufSizeShift

  private[this] var lastStream: Closeable = null
  private[this] var physicalDataFile: BlockBasedRandomAccessFile = null
  private[this] var physicalIndexFile: BlockBasedRandomAccessFile = null

  private[this] var indexBuf = new Array[Byte](indexBufSize)
  private[this] var indexBufPos = 0L // position of the start of indexBuf within this logical file
  private[this] var indexDirty = false

  private[this] var dataBuf = new Array[Byte](dataBufSize)
  private[this] var count = 1L // number of bytes written to the logical file; we need to skip the first.
  private[this] var dataBufPos = 0L // position of the start of dataBuf within this logical file
  private[this] var dataDirty = false

  private[this] var nonLinearIndexSeeks = 0L
  private[this] var nonLinearDataSeeks = 0L
  private[this] var recordBound = 0L

  def recordCount = recordBound

  def close() {
    try {
      if(physicalDataFile != null) physicalDataFile.close()
    } finally {
      try {
        if(physicalIndexFile != null) physicalIndexFile.close()
      } finally {
        if(lastStream != null) lastStream = null
      }
    }
  }

  def stats =
    IndexedTempFileStats(
      if(physicalIndexFile == null) 0 else physicalIndexFile.reads,
      if(physicalIndexFile == null) 0 else physicalIndexFile.writes,
      nonLinearIndexSeeks,
      if(physicalDataFile == null) 0 else physicalDataFile.reads,
      if(physicalDataFile == null) 0 else physicalDataFile.writes,
      nonLinearDataSeeks
    )

  /** Get an input stream associated with the given record, if it has
    * been created.
    *
    * The stream returned will be closed by the next call to `readRecord`,
    * `newRecord`, or this object's `close`.  It can also be closed explicitly.
    * It is guaranteed to have an `available` method that returns either the
    * number of bytes remaining in the record or `Int.MaxValue` if there is more
    * than fits in an `Int`.  It will also have an unbounded capacity for marking.
    */
  def readRecord(id: Long): Option[InputStreamWithWriteTo] = {
    if(lastStream != null) lastStream.close()
    ensureIndexBlockContaining(id)
    val idxPos = (id * 16 - indexBufPos).toInt

    val pos = longAt(idxPos)
    val recordSize = longAt(idxPos + 8)

    if(pos == 0) None
    else {
      val blockStartIdx = ensureDataBlockContaining(pos)
      val result = new RecordInputStream(pos, recordSize, blockStartIdx)
      lastStream = result
      Some(result)
    }
  }

  private def longAt(idxPos: Int): Long = {
    (indexBuf(idxPos).toLong << 56) |
      ((indexBuf(idxPos + 1).toLong & 0xff) << 48) |
      ((indexBuf(idxPos + 2).toLong & 0xff) << 40) |
      ((indexBuf(idxPos + 3).toLong & 0xff) << 32) |
      ((indexBuf(idxPos + 4).toLong & 0xff) << 24) |
      ((indexBuf(idxPos + 5).toLong & 0xff) << 16) |
      ((indexBuf(idxPos + 6).toLong & 0xff) << 8) |
      (indexBuf(idxPos + 7).toLong & 0xff)
  }

  /** Create an output stream that can be used to write into the given record.
    * If that record has already been created, it is replaced.
    *
    * The stream returned will be closed by the next call to `readRecord`,
    * `newRecord`, or this object's `close`.  It can also be closed explicitly.
    */
  def newRecord(id: Long): OutputStream = {
    if(lastStream != null) lastStream.close()
    recordStartOfRecord(id)
    val startIdx = ensureDataBlockContaining(count)
    val result = new RecordOutputStream(id, startIdx)
    lastStream = result
    recordBound = Math.max(recordBound, id + 1)
    result
  }

  private def storeLongAt(bs: Array[Byte], writePtr: Int, x: Long) {
    bs(writePtr) = (x >> 56).toByte
    bs(writePtr + 1) = (x >> 48).toByte
    bs(writePtr + 2) = (x >> 40).toByte
    bs(writePtr + 3) = (x >> 32).toByte
    bs(writePtr + 4) = (x >> 24).toByte
    bs(writePtr + 5) = (x >> 16).toByte
    bs(writePtr + 6) = (x >> 8).toByte
    bs(writePtr + 7) = x.toByte
  }

  private def recordStartOfRecord(id: Long) {
    val writePtr = ensureIndexBlockContaining(id)
    storeLongAt(indexBuf, writePtr, count)
    indexDirty = true
  }

  private def recordLengthOfRecord(id: Long, length: Long) {
    val writePtr = ensureIndexBlockContaining(id)
    storeLongAt(indexBuf, writePtr+8, length)
    indexDirty = true
  }

  private def ensureIndexBlockContaining(id: Long): Int = {
    val targetPos = id << 4
    if(indexBufPos > targetPos || targetPos >= indexBufPos + indexBufSize) {
      loadIndexBlockContaining(targetPos)
    }
    (targetPos - indexBufPos).toInt
  }

  private def ensureDataBlockContaining(targetPos: Long): Int = {
    if(dataBufPos > targetPos || targetPos >= dataBufPos + dataBufSize) {
      loadDataBlockContaining(targetPos)
    }
    (targetPos - dataBufPos).toInt
  }

  private def advanceToNextBlock() {
    flushData()
    physicalDataFile.nextBlock()
    dataBufPos = physicalDataFile.blockPosition
  }

  private def loadIndexBlockContaining(targetPos: Long) {
    flushIndices()
    val targetBlock = targetPos >>> indexBufSizeShift
    if(targetBlock != physicalIndexFile.blockNumber + 1) nonLinearIndexSeeks += 1
    physicalIndexFile.readBlock(targetBlock)
    indexBufPos = targetBlock << indexBufSizeShift
  }

  private def loadDataBlockContaining(targetPos: Long) {
    flushData()
    val targetBlock = targetPos >>> dataBufSizeShift
    if(targetBlock != physicalDataFile.blockNumber + 1) nonLinearDataSeeks += 1
    physicalDataFile.readBlock(targetBlock)
    dataBufPos = targetBlock << dataBufSizeShift
  }

  private def flushIndices() {
    if(physicalIndexFile == null) openPhysicalIndexFile()
    if(indexDirty) {
      physicalIndexFile.writeBlock()
      indexDirty = false
    }
  }

  private def flushData() {
    if(physicalDataFile == null) openPhysicalDataFile()
    if(dataDirty) {
      physicalDataFile.writeBlock()
      dataDirty = false
    }
  }

  private def openRAF(filename: File, shift: Int): BlockBasedRandomAccessFile = {
    val RAF = new RandomAccessFile(filename, "rw")
    try {
      new BlockBasedRandomAccessFile(shift, RAF)
    } catch {
      case e: Throwable =>
        RAF.close()
        throw e
    }
  }

  private def openPhysicalIndexFile() {
    val filename = File.createTempFile("tmp",".idx", tmpDir)
    try {
      physicalIndexFile = openRAF(filename, indexBufSizeShift)
      System.arraycopy(indexBuf, 0, physicalIndexFile.block, 0, indexBufSize)
      indexBuf = physicalIndexFile.block
    } finally {
      filename.delete()
    }
  }

  private def openPhysicalDataFile() {
    val filename = File.createTempFile("tmp",".dat", tmpDir)
    try {
      physicalDataFile = openRAF(filename, dataBufSizeShift)
      System.arraycopy(dataBuf, 0, physicalDataFile.block, 0, dataBufSize)
      dataBuf = physicalDataFile.block
    } finally {
      filename.delete()
    }
  }

  // output stream
  private[this] var recordSize: Long = _
  private[this] var dataBufWritePtr: Int = _

  private[this] def doWrite(stream: RecordOutputStream, b: Int) {
    checkClosed(stream)
    if(dataBufWritePtr == dataBufSize) doFlush()
    dataBuf(dataBufWritePtr) = b.toByte
    dataDirty = true
    dataBufWritePtr += 1
    count += 1
    recordSize += 1
  }

  private[this] def doWrite(stream: RecordOutputStream, bs: Array[Byte], start: Int, length: Int) {
    checkClosed(stream)

    def actuallyDoWrite(start: Int, length: Int) {
      System.arraycopy(bs, start, dataBuf, dataBufWritePtr, length)
      dataDirty = true
      dataBufWritePtr += length
      count += length
      recordSize += length
    }

    @tailrec
    def writeBlocks(start: Int, length: Int) {
      val remainingBuffer = dataBufSize - dataBufWritePtr
      if(remainingBuffer < length) {
        actuallyDoWrite(start, remainingBuffer)
        doFlush()
        writeBlocks(start + remainingBuffer, length - remainingBuffer)
      } else {
        actuallyDoWrite(start, length)
      }
    }

    writeBlocks(start, length)
  }

  private[this] def doFlush() {
    if(physicalDataFile == null) openPhysicalDataFile()
    physicalDataFile.writeBlock()
    physicalDataFile.nextBlock()
    dataBufPos = physicalDataFile.blockPosition
    dataBufWritePtr = 0
  }

  private[this] def checkClosed(stream: AnyRef) {
    if(lastStream ne stream) throw new IOException("Stream is closed")
  }

  private final class RecordOutputStream(recordId: Long, startIdx: Int) extends OutputStream {
    recordSize = 0L
    dataBufWritePtr = startIdx

    def write(b: Int) {
      doWrite(this, b)
    }

    override def write(bs: Array[Byte], start: Int, length: Int) {
      doWrite(this, bs, start, length)
    }

    override def flush() {
      checkClosed(this)
    }

    override def close() {
      if(lastStream eq this) {
        lastStream = null
        recordLengthOfRecord(recordId, recordSize)
      }
    }
  }

  private class RecordInputStream(startPos: Long,
                                  recordSize: Long,
                                  private[this] var blockReadIdx: Int)
    extends InputStreamWithWriteTo
  {
    private[this] val blockSize = dataBufSize
    private[this] var recordRemaining = recordSize
    private[this] var markPos = startPos

    override def close() {
      if(lastStream eq this) lastStream = null
    }

    override def markSupported = true

    override def mark(readlimit: Int) {
      checkClosed(this)
      markPos = startPos + (recordSize - recordRemaining)
    }

    override def reset() {
      checkClosed(this)
      blockReadIdx = ensureDataBlockContaining(markPos)
      recordRemaining = recordSize - (markPos - startPos)
    }

    def writeTo(that: OutputStream): Long = {
      val willHaveWritten = recordRemaining
      while(recordRemaining > 0) {
        if(blockReadIdx == blockSize) refill()
        val blockRemaining = Math.min(recordRemaining, blockSize - blockReadIdx).toInt
        that.write(dataBuf, blockReadIdx, blockRemaining)
        blockReadIdx += blockRemaining
        recordRemaining -= blockRemaining
      }
      willHaveWritten
    }

    def read(): Int = {
      checkClosed(this)
      if(recordRemaining == 0) return -1
      if(blockReadIdx == blockSize) refill()
      val result = dataBuf(blockReadIdx)
      blockReadIdx += 1
      recordRemaining -= 1
      result & 0xff
    }

    override def available = {
      checkClosed(this)
      Math.min(Int.MaxValue, recordRemaining).toInt
    }

    private def refill() {
      advanceToNextBlock()
      blockReadIdx = 0
    }

    override def read(bs: Array[Byte], start: Int, length: Int): Int = {
      checkClosed(this)
      val trueLength = Math.min(length, recordRemaining).toInt
      val remainingBuffer = blockSize - blockReadIdx
      if(recordRemaining == 0) {
        -1
      } else if(trueLength <= remainingBuffer) {
        System.arraycopy(dataBuf, blockReadIdx, bs, start, trueLength)
        blockReadIdx += trueLength
        recordRemaining -= trueLength
        trueLength
      } else {
        if(remainingBuffer != 0) {
          System.arraycopy(dataBuf, blockReadIdx, bs, start, remainingBuffer)
          blockReadIdx += remainingBuffer
          recordRemaining -= remainingBuffer
        }
        continueRead(bs, start + remainingBuffer, trueLength - remainingBuffer, remainingBuffer)
      }
    }

    private def continueRead(bs: Array[Byte], start: Int, length: Int, soFar: Int): Int = {
      // precondition: emptied the buffer
      assert(blockSize == blockReadIdx)
      if(recordRemaining == 0) return soFar
      refill()
      val remainingBuffer = blockSize - blockReadIdx
      if(remainingBuffer < length) {
        System.arraycopy(dataBuf, blockReadIdx, bs, start, remainingBuffer)
        recordRemaining -= remainingBuffer
        blockReadIdx += remainingBuffer
        continueRead(bs, start + remainingBuffer, length - remainingBuffer, soFar + remainingBuffer)
      } else {
        System.arraycopy(dataBuf, blockReadIdx, bs, start, length)
        recordRemaining -= length
        blockReadIdx += length
        soFar + length
      }
    }
  }
}

object Test extends App {
  val f = new IndexedTempFile(160, 2000)

  def dump(record: Long) {
    f.readRecord(record) match {
      case None => println("No such record " + record + "?")
      case Some(x) =>
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        x.mark(Int.MaxValue)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        x.reset()
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
        println(x.read().toChar)
    }
  }

  f.newRecord(0).write("0123456789abcdef".getBytes)
  f.newRecord(3).write("0123456789abcdef".reverse.getBytes)
  f.newRecord(1).write("0123456789abcdef".getBytes)
  dump(0)
  dump(1)
  dump(3)
  f.close()
}

abstract class InputStreamWithWriteTo extends InputStream {
  def writeTo(that: OutputStream): Long
}
