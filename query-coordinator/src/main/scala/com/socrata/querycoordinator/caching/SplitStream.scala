package com.socrata.querycoordinator.caching

import java.io._

import com.rojoma.simplearm.v2._

import scala.annotation.tailrec

object SplitStream {
  trait Introspector { // for tests
  def filesCreated: Int
    def bytesRead: Long
    def bytesSpilled: Long
  }

  /** Duplicate the given `inputStream`.  The input stream is consumed and should be
    * considered invalid.  The two output streams are independent (and in particular,
    * may be read from two separate threads).
    *
    * @param in The input stream to be duplicated.
    * @param bufSize The amount of data to retain in-memory before spilling to disk.
    * @param rs Manages the returned streams.
    * @param tmpDir The directory in which to place temporary files.  Must already exist and be writable.  If `None`,
    *               the system temporary directory is used.
    * @param transitiveCloseIn If true (the default) the input stream will be closed when both the new streams are closed.
    * @param streamWrapper A policy for wrapping the temporary files, for example to compress them.  This should only
    *                      be used if the stream consumers are expected to have significantly different speeds.
    * @return Two `InputStream`s with the same contents as `in`.
    */
  def apply(in: InputStream,
            bufSize: Int,
            rs: ResourceScope,
            tmpDir: Option[File] = None,
            transitiveCloseIn: Boolean = true,
            streamWrapper: StreamWrapper = StreamWrapper.noop): (InputStream with Introspector, InputStream with Introspector) = {
    val subscope = new ResourceScope("splitstream")
    if(transitiveCloseIn) subscope.open(in)
    val globalLock = new Object

    // There are at most two files on the disk at any time.
    //   0: both the streams are being read at roughly the same rate, and they
    //      are within `bufSize` bytes of each other
    //   1 (deleted): A slow reader is consuming data; the fast reader is not
    //      causing any more to be written
    //   1 (linked): A fast reader is causing data to be buffered for a slow
    //      reader, which has not yet tried to read it.
    //   2 (one linked, one deleted): A fast reader is causing data to be
    //      buffered, and a slow reader is reading it.
    // Reading only ever happens from a deleted file.  Writing only ever happens
    // to a linked one.  The two are represented by `laggardStream` and
    // `pendingFile` respectively.

    // Locking:
    // Each output stream has a local lock.  In addition, they both share a "global"
    // lock.  The local lock is taken on all entries to the stream (read and close,
    // basically).  The global lock is taken at need.

    var laggardStream: BufferedInputStream = null // this is only accessed by the laggard, NEVER by the speedy (and hence does not require the global lock)

    var pendingFile: File = null // this is only ever accessed while holding the global lock                       \ Either both of these are null
    var pendingFileStream: BufferedOutputStream = null // this is only ever accessed while holding the global lock / or neither.

    var pendingFileCount = 0 // total number of files created; only accessed while holding the global lock
    var sawEOF = false // this is only ever accessed while holding the global lock

    // These are read-only after construction.
    val streams = new Array[SharedInputStream with Introspector](2)

    val buffer = new CircularByteBuffer(bufSize) // only ever accessed while holding the global lock
    var totalRead = 0L // only ever accessed while holding the global lock; total bytes read from the input stream
    var totalSpilled = 0L // only ever accessed while holding the global lock; total bytes written to temp files
    var closedCount = 0 // only ever accessed while holding the global lock; count of the return streams which have been closed.

    var pendingException: IOException = null

    class SharedInputStream(otherIndex: Int) extends InputStream with Introspector {
      def bytesRead = globalLock.synchronized { totalRead }
      def filesCreated = globalLock.synchronized { pendingFileCount }
      def bytesSpilled = globalLock.synchronized { totalSpilled }

      private val localLock = new Object
      @volatile private var isLaggard = false // this can turn from false to true asynchronously, but will never go the other way
      private var offset: Long = 0L
      private var closed = false
      private var broken = false // true if an IO exception was encountered while reading

      private def other = streams(otherIndex)

      def read(): Int = {
        val b = new Array[Byte](1)
        read(b) match {
          case -1 => -1
          case 1 => b(0) & 0xff
          case _ => sys.error("Internal error: read returned neither EOF nor at least one byte read")
        }
      }

      override def read(bs: Array[Byte]): Int = read(bs, 0, bs.length)

      override def read(bs: Array[Byte], off: Int, len: Int): Int = {
        // random boilerplate to make this work exactly like InputStream#read
        if (bs eq null) {
          throw new NullPointerException()
        } else if (off < 0 || len < 0 || len > bs.length - off) {
          throw new IndexOutOfBoundsException()
        } else if (len == 0) {
          return 0
        }

        localLock.synchronized {
          if(closed) throw new IOException("read from a closed file")
          doRead(bs, off, len) match {
            case -1 => -1
            case n => offset += n; n
          }
        }
      }

      @tailrec
      private def doRead(bs: Array[Byte], off: Int, len: Int): Int =
        if(isLaggard) {
          if(laggardStream eq null) { // first time, we need to open the pending file
            globalLock.synchronized {
              assert(pendingFile ne null)
              assert(pendingFileStream ne null)
              subscope.close(pendingFileStream)
              val rootStream = subscope.open(new FileInputStream(pendingFile))
              val wrappedStream = streamWrapper.wrapInputStream(rootStream, subscope)
              laggardStream = subscope.open(new BufferedInputStream(wrappedStream), transitiveClose = List(wrappedStream))
              pendingFile.delete()
              pendingFile = null
              pendingFileStream = null
            }
          }
          laggardStream.read(bs, off, len) match {
            case -1 =>
              // slow path -- we might become speedy, we might switch to pending file
              globalLock.synchronized {
                subscope.close(laggardStream)
                laggardStream = null
                if(pendingFile eq null) {
                  // ok, we're fast now
                  isLaggard = false
                } else {
                  // nope, still slow; we'll open the pending file on the next pass around
                  laggardStream = null
                }
              }
              doRead(bs, off, len)
            case n =>
              n
          }
        } else {
          globalLock.synchronized {
            if(!isLaggard) return readFromBuffer(bs, off, len)
          }
          // we became a laggard in between the first check and acquiring the lock (which we have now released)
          doRead(bs, off, len)
        }

      private def readFromBuffer(bs: Array[Byte], off: Int, len: Int): Int = {
        if(offset == totalRead) { // I'm at the front of the file
          if(sawEOF) return -1
          actuallyRead(bs, off, len) match {
            case -1 =>
              sawEOF = true
              -1
            case n =>
              if(closedCount == 0) {
                addToBuffer(bs, off, n)
              }
              totalRead += n
              n
          }
        } else { // I'm _slightly_ lagging
          buffer.get(bs, off, len)
        }
      }

      private def actuallyRead(bs: Array[Byte], off: Int, len: Int): Int = {
        if(broken) throw new IOException("Attempt to read from broken stream")
        if(pendingException != null) {
          broken = true
          throw new RethrownIOException(pendingException)
        }
        try {
          in.read(bs, off, len)
        } catch {
          case e: IOException =>
            pendingException = e
            broken = true
            throw e
        }
      }

      private def addToBuffer(bs: Array[Byte], off: Int, len: Int) {
        if(len > bufSize) {
          // The requested chunk can't possibly fit in the buffer,
          // so just never put it there.  Write it straight to the
          // temp storage.
          spillToDisk()
          pendingFileStream.write(bs, off, len)
          totalSpilled += len
        } else {
          // It'll fit if there's room (but there might not be).
          val stored = buffer.put(bs, off, len)
          if(stored != len) {
            // There wasn't.  Spill the buffer to disk and bufferize the leftovers
            spillToDisk()
            val stored2 = buffer.put(bs, off + stored, len - stored)
            assert(stored2 == len - stored)
          }
        }
      }

      private def spillToDisk() {
        ensurePending()
        totalSpilled += buffer.available
        buffer.writeTo(pendingFileStream)
        other.isLaggard = true
      }

      private def ensurePending() {
        if(pendingFile eq null) {
          assert(pendingFileStream eq null)
          pendingFile = File.createTempFile("tmp", ".dat", tmpDir.orNull)
          pendingFileCount += 1
          val rootStream = subscope.open(new FileOutputStream(pendingFile))
          val wrappedStream = streamWrapper.wrapOutputStream(rootStream, subscope)
          pendingFileStream = subscope.open(new BufferedOutputStream(wrappedStream), transitiveClose = List(wrappedStream))
        } else {
          assert(pendingFileStream ne null)
        }
      }

      override def close() {
        localLock.synchronized {
          if(!closed) {
            closed = true
            globalLock.synchronized {
              closedCount += 1
              if(isLaggard) { // don't need the saved data anymore
                if(laggardStream ne null) {
                  subscope.close(laggardStream)
                  laggardStream = null
                }
                if(pendingFile ne null) {
                  subscope.close(pendingFileStream)
                  pendingFile.delete()
                  pendingFile = null
                  pendingFileStream = null
                }
              }
              if(closedCount == 2) subscope.close()
            }
          }
        }
      }
    }

    streams(0) = rs.open(new SharedInputStream(1))
    streams(1) = rs.open(new SharedInputStream(0))

    (streams(0), streams(1))
  }
}
