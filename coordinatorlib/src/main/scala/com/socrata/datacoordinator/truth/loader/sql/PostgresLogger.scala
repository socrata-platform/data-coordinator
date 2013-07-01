package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.util.TimingReport
import java.io._
import java.nio.charset.StandardCharsets
import com.rojoma.simplearm.util._
import java.nio.ByteBuffer

class PostgresLogger[CT, CV](connection: Connection,
                             logTableName: String,
                             rowCodecFactory: () => RowLogCodec[CV],
                             timingReport: TimingReport,
                             copyIn: (Connection, String, (OutputStream => Unit)) => Long,
                             tmpDir: File,
                             rowFlushSize: Int = 128000)
  extends AbstractSqlLogger[CT, CV](connection, logTableName, rowCodecFactory, timingReport, rowFlushSize)
{
  import PostgresLogger._

  private[this] var tmp: RandomAccessFile = _
  private[this] var tmpWrapped: DataOutputStream = _

  private[this] val initialBatchSize = lastBatchSize
  private[this] var batchSize = initialBatchSize
  private[this] var wrote = 0L
  private[this] val targetTimeInNanos = 1000000000L.toDouble // 1 second

  override def close() {
    if(batchSize != initialBatchSize) lastBatchSize = batchSize
    tmp.close()
    tmp = null; tmpWrapped = null // force errors if re-used
  }

  private def openTmp() {
    assert(tmp == null)
    val filename = File.createTempFile("log",".tmp", tmpDir)
    try {
      tmp = new RandomAccessFile(filename, "rw")
      try {
        wrote = 0L
        tmpWrapped = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmp.getFD)))
        writeStart()
      } catch {
        case e: Throwable =>
          tmp.close()
          tmp = null
          throw e
      }
    } finally {
      filename.delete()
    }
  }

  private val copyInSql =
    s"COPY $logTableName (version,subversion,what,aux) FROM STDIN WITH (FORMAT 'binary')"

  private def writeStart() {
    tmpWrapped.write(binaryFormatHeader)
    wrote += binaryFormatHeader.length
  }

  private def writeEntry(version: Long, subversion: Long, what: Array[Byte], aux: Array[Byte]) {
    tmpWrapped.writeShort(4) // number of values
    wrote += 2

    tmpWrapped.writeInt(8) // sizeof(long)
    tmpWrapped.writeLong(version)
    wrote += 12

    tmpWrapped.writeInt(8)
    tmpWrapped.writeLong(subversion)
    wrote += 12

    tmpWrapped.writeInt(what.length)
    tmpWrapped.write(what)
    wrote += 4 + what.length

    tmpWrapped.writeInt(aux.length)
    tmpWrapped.write(aux)
    wrote += 4 + aux.length
  }

  private def writeEnd() {
    tmpWrapped.writeShort(-1)
  }

  private def reopenTmp() {
    tmp.close()
    tmp = null; tmpWrapped = null // force errors if openTmp fails and this is reused
    openTmp()
  }

  protected def flushBatch() {
    writeEnd()
    tmpWrapped.flush()

    timingReport("write-log", "log-table" -> logTableName) {
      copyIn(connection, copyInSql, { out =>
        using(tmp.getChannel) { chan =>
          val arr = new Array[Byte](10240)
          val buf = ByteBuffer.wrap(arr)
          def loop(offset: Long) {
            buf.clear()
            chan.read(buf, offset) match {
              case -1 => // done
              case n => out.write(arr, 0, n); loop(offset + n)
            }
          }
          loop(0L)
          reopenTmp()
        }
      })
    }
  }

  private def maybeFlushBatch() {
    if(wrote >= batchSize) {
      val start = System.nanoTime()
      flushBatch()
      val end = System.nanoTime()
      val delta = end - start
      val mult = Math.max(0.5, Math.min(2.0, targetTimeInNanos / delta.toDouble))
      batchSize = (batchSize * mult).toLong
      log.info("Setting batch size to {}", batchSize)
    }
  }

  protected def logLine(what: String, aux: Array[Byte]) {
    writeEntry(versionNum, nextSubVersionNum(), bytesOf(what), aux)
    maybeFlushBatch()
  }

  openTmp()
}

object PostgresLogger {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresLogger[_,_]])

  @volatile var lastBatchSize = 1000000L

  val binaryFormatHeader = "PGCOPY\n\u00ff\r\n\0\0\0\0\0\0\0\0\0".getBytes(StandardCharsets.ISO_8859_1)
  //          header proper ^^^^^^^^^^^^^^^^^^^^
  //                                      flags ^^^^^^^^
  //                            header extension length ^^^^^^^^

  val bytesOf = SqlLogger.allEvents.map { s => s -> s.getBytes(StandardCharsets.UTF_8) }.toMap
}
