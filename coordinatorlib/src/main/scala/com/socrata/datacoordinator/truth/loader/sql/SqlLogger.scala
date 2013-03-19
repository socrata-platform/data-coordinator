package com.socrata.datacoordinator
package truth.loader
package sql

import scala.io.Codec

import java.sql.{PreparedStatement, Connection}

import com.rojoma.json.util.JsonUtil
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{TimingReport, Counter}
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.RowId

class SqlLogger[CV](connection: Connection,
                    logTableName: String,
                    rowCodecFactory: () => RowLogCodec[CV],
                    timingReport: TimingReport,
                    rowFlushSize: Int = 128000,
                    batchFlushSize: Int = 2000000)
  extends Logger[CV]
{
  import SqlLogger.log

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

  val nullBytes = Codec.toUTF8("null")
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

  def logLine(what: String, data: Array[Byte]) {
    val i = insertStmt
    i.setLong(1, nextSubVersionNum())
    i.setString(2, what)
    i.setBytes(3, data)
    i.addBatch()

    totalSize += data.length
    batched += 1
  }

  def logLine(what: String, data: String) {
    logLine(what, Codec.toUTF8(data))
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
    logLine(SqlLogger.Truncated, nullBytes)
  }

  def columnCreated(info: ColumnInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.ColumnCreated, JsonUtil.renderJson(info.unanchored))
  }

  def columnRemoved(info: ColumnInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.ColumnRemoved, JsonUtil.renderJson(info.unanchored))
  }

  def rowIdentifierSet(info: ColumnInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.RowIdentifierSet, JsonUtil.renderJson(info.unanchored))
  }

  def rowIdentifierCleared(info: ColumnInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.RowIdentifierCleared, JsonUtil.renderJson(info.unanchored))
  }

  def systemIdColumnSet(info: ColumnInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.SystemRowIdentifierChanged, JsonUtil.renderJson(info.unanchored))
  }

  def logicalNameChanged(info: ColumnInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.ColumnLogicalNameChanged, JsonUtil.renderJson(info.unanchored))
  }

  def workingCopyCreated(info: CopyInfo) {
    checkTxn()
    flushRowData()
    val datasetJson = JsonUtil.renderJson(info.datasetInfo.unanchored)
    val versionJson = JsonUtil.renderJson(info.unanchored)
    logLine(SqlLogger.WorkingCopyCreated, datasetJson + "\n" + versionJson)
  }

  def dataCopied() {
    checkTxn()
    flushRowData()

    logLine(SqlLogger.DataCopied, nullBytes)
  }

  def snapshotDropped(info: CopyInfo) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.SnapshotDropped, JsonUtil.renderJson(info.unanchored))
  }

  def workingCopyDropped() {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.WorkingCopyDropped, nullBytes)
  }

  def workingCopyPublished() {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.WorkingCopyPublished, nullBytes)
  }

  def endTransaction() = {
    checkTxn()
    transactionEnded = true

    flushRowData()

    if(nextSubVersionNum.peek != 1) {
      logLine(SqlLogger.TransactionEnded, nullBytes)
      flushBatch()
      Some(versionNum)
    } else {
      None
    }
  }

  // DataLogger facet starts here

  var baos: java.io.ByteArrayOutputStream = _
  var sos: org.xerial.snappy.SnappyOutputStream = _
  var out: com.google.protobuf.CodedOutputStream = _
  var rowCodec: RowLogCodec[CV] = _
  var didOne: Boolean = _
  reset()

  def reset() {
    baos = new java.io.ByteArrayOutputStream
    baos.write(0) // "we're using Snappy"
    sos = new org.xerial.snappy.SnappyOutputStream(baos)
    out = com.google.protobuf.CodedOutputStream.newInstance(sos)
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
      sos.flush()
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

  def rowIdCounterUpdated(nextRowId: RowId) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.RowIdCounterUpdated, nextRowId.underlying.toString)
  }

  def close() {
    if(_insertStmt != null) _insertStmt.close()
  }
}

object SqlLogger {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLogger[_]])

  // all of these must be at most 8 characters long and consist of
  // nothing but lower-case ASCII letters.
  val RowDataUpdated = "rowdata"
  val RowIdCounterUpdated = "rowcnter"

  val Truncated = "truncate"
  val ColumnCreated = "colcreat"
  val ColumnRemoved = "coldel"
  val RowIdentifierSet = "ridcol"
  val RowIdentifierCleared = "noridcol"
  val SystemRowIdentifierChanged = "sidcol"
  val ColumnLogicalNameChanged = "colname"
  val WorkingCopyCreated = "workcopy"
  val DataCopied = "datacopy"
  val WorkingCopyDropped = "dropwork"
  val SnapshotDropped = "dropsnap"
  val WorkingCopyPublished = "pubwork"
  val TransactionEnded = "endtxn"

  val maxOpLength = 8

  private def good(s: String) = s.length <= maxOpLength && s.forall { c => c >= 'a' && c <= 'z' }

  for {
    method <- getClass.getDeclaredMethods
    if java.lang.reflect.Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
    if method.getReturnType == classOf[String]
  } assert(good(method.invoke(this).asInstanceOf[String]), s"${method.getName} is either more than $maxOpLength characters long or contains something which isn't a lowercase letter")
}
