package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.util.{Counter, TimingReport}
import java.io._
import java.nio.charset.StandardCharsets
import com.rojoma.simplearm.util._
import java.nio.ByteBuffer
import com.socrata.datacoordinator.id.RowId
import java.util.zip.{Deflater, DeflaterOutputStream}
import com.google.protobuf.MessageLite
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo

class PostgresLogger[CT, CV](connection: Connection,
                             logTableName: String,
                             rowCodecFactory: () => RowLogCodec[CV],
                             timingReport: TimingReport,
                             copyIn: (Connection, String, (OutputStream => Unit)) => Long,
                             tmpDir: File,
                             rowFlushSize: Int = 128000)
  extends Logger[CT, CV]
{
  import PostgresLogger._

  import messages.ToProtobuf._

  private[this] var tmp: RandomAccessFile = _
  private[this] var tmpWrapped: DataOutputStream = _

  lazy val versionNum = timingReport("version-num", "log-table" -> logTableName) {
    for {
      stmt <- managed(connection.createStatement())
      rs <- managed(stmt.executeQuery("SELECT MAX(version) FROM " + logTableName))
    } yield {
      val hasNext = rs.next()
      assert(hasNext, "next version query didn't return anything?")
      // MAX(version) will be null if there is no data in the log table;
      // ResultSet#getLong returns 0 if the value was null.
      rs.getLong(1) + 1
    }
  }

  override def close() {
    tmp.close()
    if(batchSize != initialBatchSize) lastBatchSize = batchSize
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

  private val initialBatchSize = lastBatchSize
  private var batchSize = initialBatchSize
  private var wrote = 0L
  private val targetTimeInNanos = 1000000000L.toDouble // 1 second
  private val nextSubVersionNum = new Counter(init = 1)
  private var transactionEnded = false

  val copyInSql =
    s"COPY $logTableName (version,subversion,what,aux) FROM STDIN WITH (FORMAT 'binary')"

  def writeStart() {
    tmpWrapped.write(binaryFormatHeader)
    wrote += binaryFormatHeader.length
  }

  def writeEntry(version: Long, subversion: Long, what: Array[Byte], aux: Array[Byte]) {
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

    maybeFlush()
  }

  def writeEnd() {
    tmpWrapped.writeShort(-1)
  }

  private def reopenTmp() {
    tmp.close()
    tmp = null; tmpWrapped = null // force errors if openTmp fails and this is reused
    openTmp()
  }

  private def flush() {
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

  private def maybeFlush() {
    if(wrote >= batchSize) {
      val start = System.nanoTime()
      flush()
      val end = System.nanoTime()
      val delta = end - start
      val mult = Math.max(0.5, Math.min(2.0, targetTimeInNanos / delta.toDouble))
      batchSize = (batchSize * mult).toLong
      log.info("Setting batch size to {}", batchSize)
    }
  }

  def checkTxn() {
    assert(!transactionEnded, "Operation logged after saying the transaction was over")
  }

  def logLine(what: Array[Byte], aux: Array[Byte]) {
    writeEntry(versionNum, nextSubVersionNum(), what, aux)
  }

  def logLine(what: Array[Byte], aux: MessageLite) {
    logLine(what, aux.toByteArray)
  }

  def truncated() {
    checkTxn()
    flushRowData()
    logLine(Truncated, messages.Truncated.defaultInstance)
  }

  def columnCreated(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(ColumnCreated, messages.ColumnCreated(convert(info.unanchored)))
  }

  def columnRemoved(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(ColumnRemoved, messages.ColumnRemoved(convert(info.unanchored)))
  }

  def rowIdentifierSet(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(RowIdentifierSet, messages.RowIdentifierSet(convert(info.unanchored)))
  }

  def rowIdentifierCleared(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(RowIdentifierCleared, messages.RowIdentifierCleared(convert(info.unanchored)))
  }

  def systemIdColumnSet(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(SystemRowIdentifierChanged, messages.SystemIdColumnSet(convert(info.unanchored)))
  }

  def versionColumnSet(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(VersionColumnChanged, messages.VersionColumnSet(convert(info.unanchored)))
  }

  def logicalNameChanged(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(ColumnLogicalNameChanged, messages.LogicalNameChanged(convert(info.unanchored)))
  }

  def workingCopyCreated(info: CopyInfo) {
    checkTxn()
    flushRowData()
    logLine(WorkingCopyCreated, messages.WorkingCopyCreated(
      convert(info.datasetInfo.unanchored),
      convert(info.unanchored))
    )
  }

  def dataCopied() {
    checkTxn()
    flushRowData()

    logLine(DataCopied, messages.DataCopied.defaultInstance)
  }

  def snapshotDropped(info: CopyInfo) {
    checkTxn()
    flushRowData()
    logLine(SnapshotDropped, messages.SnapshotDropped(convert(info.unanchored)))
  }

  def workingCopyDropped() {
    checkTxn()
    flushRowData()
    logLine(WorkingCopyDropped, messages.WorkingCopyDropped.defaultInstance)
  }

  def workingCopyPublished() {
    checkTxn()
    flushRowData()
    logLine(WorkingCopyPublished, messages.WorkingCopyPublished.defaultInstance)
  }

  def endTransaction() = {
    checkTxn()
    transactionEnded = true

    flushRowData()

    if(nextSubVersionNum.peek != 1) {
      logLine(TransactionEnded, messages.EndTransaction.defaultInstance)
      flush()
      Some(versionNum)
    } else {
      None
    }
  }

  def counterUpdated(nextCounter: Long) {
    checkTxn()
    flushRowData()
    logLine(CounterUpdated, messages.CounterUpdated(nextCounter))
  }

  // DataLogger facet starts here

  var baos: java.io.ByteArrayOutputStream = _
  var underlyingOutputStream: java.io.OutputStream = _
  var out: com.google.protobuf.CodedOutputStream = _
  var rowCodec: RowLogCodec[CV] = _
  var didOne: Boolean = _
  reset()

  def reset() {
    baos = new java.io.ByteArrayOutputStream

    /*
    baos.write(0) // "we're using Snappy"
    underlyingOutputStream = new org.xerial.snappy.SnappyOutputStream(baos)
    */

    /*
    baos.write(1) // "no compression"
    underlyingOutputStream = baos
    */

    baos.write(2) // "deflate"
    underlyingOutputStream = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_SPEED)) // FIXME: close will not free this deflater's native mem.  Need to deflater.end() it.

    out = com.google.protobuf.CodedOutputStream.newInstance(underlyingOutputStream)

    didOne = false
    rowCodec = rowCodecFactory()
    rowCodec.writeVersion(out)
  }

  def maybeFlushRowData() {
    didOne=true
    if(baos.size > rowFlushSize) {
      flushRowData()
    }
  }

  def flushRowData() {
    if(didOne) {
      out.flush()
      underlyingOutputStream.close()
      val bytes = baos.toByteArray
      reset()
      logLine(RowDataUpdated, bytes)
    }
  }

  def insert(sid: RowId, row: Row[CV]) {
    checkTxn()
    rowCodec.insert(out, sid, row)
    maybeFlushRowData()
  }

  def update(sid: RowId, row: Row[CV]) {
    checkTxn()
    rowCodec.update(out, sid, row)
    maybeFlushRowData()
  }

  def delete(systemID: RowId) {
    checkTxn()
    rowCodec.delete(out, systemID)
    maybeFlushRowData()
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

  private def bin(s: String) = s.getBytes(StandardCharsets.UTF_8)

  val RowDataUpdated = bin(SqlLogger.RowDataUpdated)
  val CounterUpdated = bin(SqlLogger.CounterUpdated)

  val Truncated = bin(SqlLogger.Truncated)
  val ColumnCreated = bin(SqlLogger.ColumnCreated)
  val ColumnRemoved = bin(SqlLogger.ColumnRemoved)
  val RowIdentifierSet = bin(SqlLogger.RowIdentifierSet)
  val RowIdentifierCleared = bin(SqlLogger.RowIdentifierCleared)
  val SystemRowIdentifierChanged = bin(SqlLogger.SystemRowIdentifierChanged)
  val VersionColumnChanged = bin(SqlLogger.VersionColumnChanged)
  val ColumnLogicalNameChanged = bin(SqlLogger.ColumnLogicalNameChanged)
  val WorkingCopyCreated = bin(SqlLogger.WorkingCopyCreated)
  val DataCopied = bin(SqlLogger.DataCopied)
  val WorkingCopyDropped = bin(SqlLogger.WorkingCopyDropped)
  val SnapshotDropped = bin(SqlLogger.SnapshotDropped)
  val WorkingCopyPublished = bin(SqlLogger.WorkingCopyPublished)
  val TransactionEnded = bin(SqlLogger.TransactionEnded)
}
