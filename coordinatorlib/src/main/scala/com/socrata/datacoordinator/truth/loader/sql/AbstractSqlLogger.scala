package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection
import com.google.protobuf.MessageLite
import com.socrata.datacoordinator.truth.RowLogCodec
import java.util.zip.{Deflater, DeflaterOutputStream}
import com.socrata.datacoordinator.util.{Counter, TimingReport}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.RowId
import com.rojoma.simplearm.util._

abstract class AbstractSqlLogger[CT, CV](val connection: Connection,
                                         val logTableName: String,
                                         rowCodecFactory: () => RowLogCodec[CV],
                                         val timingReport: TimingReport,
                                         rowFlushSize: Int = 128000)
  extends Logger[CT, CV]
{
  import SqlLogger._
  import messages.ToProtobuf._

  protected def logLine(what: String, aux: Array[Byte])
  protected def flushBatch()

  protected lazy val versionNum = timingReport("version-num", "log-table" -> logTableName) {
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

  private[this] var transactionEnded = false
  protected[this] val nextSubVersionNum = new Counter(init = 1)

  private[this] def checkTxn() {
    assert(!transactionEnded, "Operation logged after saying the transaction was over")
  }

  private def logLine(what: String, aux: MessageLite) {
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

  private[this] var baos: java.io.ByteArrayOutputStream = _
  private[this] var underlyingOutputStream: java.io.OutputStream = _
  private[this] var out: com.google.protobuf.CodedOutputStream = _
  private[this] var rowCodec: RowLogCodec[CV] = _
  private[this] var didOne: Boolean = _
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

    /*
    baos.write(2) // "deflate"
    underlyingOutputStream = new DeflaterOutputStream(baos)
    */

    baos.write(3) // "pure java snappy"
    underlyingOutputStream = new org.iq80.snappy.SnappyOutputStream(baos)

    out = com.google.protobuf.CodedOutputStream.newInstance(underlyingOutputStream)

    didOne = false
    rowCodec = rowCodecFactory()
    rowCodec.writeVersion(out)
  }

  private def maybeFlushRowData() {
    didOne=true
    if(baos.size > rowFlushSize) {
      flushRowData()
    }
  }

  private def flushRowData() {
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
}
