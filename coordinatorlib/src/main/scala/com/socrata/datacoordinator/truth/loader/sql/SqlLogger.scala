package com.socrata.datacoordinator
package truth.loader
package sql

import scala.io.Codec

import java.sql.{PreparedStatement, Connection}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{TimingReport, Counter}
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.RowId
import java.io.OutputStream
import java.util.zip.{Deflater, DeflaterOutputStream}
import com.google.protobuf.MessageLite

class SqlLogger[CT, CV](connection: Connection,
                        logTableName: String,
                        rowCodecFactory: () => RowLogCodec[CV],
                        timingReport: TimingReport,
                        rowFlushSize: Int = 128000,
                        batchFlushSize: Int = 2000000)
  extends Logger[CT, CV]
{
  import SqlLogger._
  import messages.ToProtobuf._

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

  var transactionEnded = false
  val nextSubVersionNum = new Counter(init = 1)

  var _insertStmt: PreparedStatement = null

  var totalSize = 0
  var batched = 0

  def insertStmt = {
    if(_insertStmt == null) {
      _insertStmt = connection.prepareStatement("INSERT INTO " + logTableName + " (version, subversion, what, aux) VALUES (" + versionNum + ", ?, ?, ?)")
    }
    _insertStmt
  }

  def logLine(what: String, data: String) {
    logLine(what, Codec.toUTF8(data))
  }

  def logLine(what: String, data: Array[Byte]) {
    val i = insertStmt
    i.setLong(1, nextSubVersionNum())
    i.setString(2, what)
    i.setBytes(3, data)
    i.addBatch()

    totalSize += data.length
    batched += 1
  }

  def logLine(what: String, aux: MessageLite) {
    logLine(what, aux.toByteArray)
  }

  def maybeFlushBatch() {
    if(totalSize > batchFlushSize) flushBatch()
  }

  def flushBatch() {
    if(batched != 0) {
      timingReport("flush-log-batch", "count" -> batched) {
        log.debug("Flushing {} log rows", batched)

        batched = 0
        totalSize = 0

        insertStmt.executeBatch()
      }
    }
  }

  def checkTxn() {
    assert(!transactionEnded, "Operation logged after saying the transaction was over")
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
      flushBatch()
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
  var underlyingOutputStream: OutputStream = _
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
    flushInner()
    maybeFlushBatch()
  }

  def flushInner() {
    if(didOne) {
      out.flush()
      underlyingOutputStream.close()
      val bytes = baos.toByteArray
      reset()
      logLine(SqlLogger.RowDataUpdated, bytes)
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

  def close() {
    if(_insertStmt != null) {
      _insertStmt.close()
      _insertStmt = null
    }
  }
}

object SqlLogger {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLogger[_,_]])

  val maxOpLength = 8

  // all of these must be at most 8 characters long and consist of
  // nothing but lower-case ASCII letters.
  val RowDataUpdated = "rowdata"
  val CounterUpdated = "cnterup"

  val Truncated = "truncate"
  val ColumnCreated = "colcreat"
  val ColumnRemoved = "coldel"
  val RowIdentifierSet = "ridcol"
  val RowIdentifierCleared = "noridcol"
  val SystemRowIdentifierChanged = "sidcol"
  val VersionColumnChanged = "vercol"
  val ColumnLogicalNameChanged = "colname"
  val WorkingCopyCreated = "workcopy"
  val DataCopied = "datacopy"
  val WorkingCopyDropped = "dropwork"
  val SnapshotDropped = "dropsnap"
  val WorkingCopyPublished = "pubwork"
  val TransactionEnded = "endtxn"

  val allEvents = for {
    method <- getClass.getDeclaredMethods
    if java.lang.reflect.Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
    if method.getReturnType == classOf[String]
  } yield method.invoke(this).asInstanceOf[String]
}
